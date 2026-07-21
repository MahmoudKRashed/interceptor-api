# Tanfeeth Service Interceptor — Build Specification (Java Microservices / Spring Boot)

**Audience:** an AI agent (and engineers) rebuilding the existing IBM IIB/ACE
interceptor in Java 17+ / Spring Boot 3.x microservices.
**Intent:** describe the **business logic and integration contracts** precisely
and technology-neutrally, so the same behaviour can be reproduced on a different
stack. The current implementation is IBM ACE/ESQL over IBM MQ + DB2/MSSQL; this
document abstracts that into requirements plus a suggested Spring design.

> Rule of authority: where a business rule and an implementation note conflict,
> the **business rule wins**. Implementation notes (queue names, ESQL specifics)
> are guidance, not contract, unless marked **MUST**.

---

## 1. Context & purpose

The **Saudi Central Bank (SAMA) Tanfeeth** system sends financial execution
requests (block, garnish, lift, transfer, deduction, inquiries, amount updates,
death notifications, etc.) to the bank as SOAP/XML over HTTPS. The interceptor
sits in front of the bank's existing ("old") Tanfeeth processing to **offload
traffic**: it decides whether each request concerns a **bank customer** or a
**non-customer**, and:

- **Customer** → forward the request unchanged to the existing Tanfeeth service.
- **Non-customer** → auto-respond to SAMA via an asynchronous callback (so the
  old system never sees it), and for restraint operations maintain an internal
  compliance block-list (ANTS/SISL).

It also maintains a **relation registry** so follow-up messages (reverse, lift,
override, amount-update) that act on a previously-seen transaction are resolved
without re-checking, and routes them to the **same backend** that served the
original (DB2 primary vs MSSQL DR).

### 1.1 Ingress path (unchanged in the rebuild)
```
SAMA → APIC External (SJN, DMZ) → Internal API → APIC Internal (esb-dp-<env>) → Interceptor
```
The interceptor exposes a single HTTPS endpoint the internal APIC invokes.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| SRN | SAMA reference number — identifies a transaction |
| MsgUID | SAMA message unique id |
| OvrdMsgUID | Present on an **override** message; the MsgUID it overrides |
| Mod | SAMA header modifier; `Mod < 0` = a **reversal** |
| Lift | An operation that releases a prior restraint (e.g. `FILiftRq`) |
| UpdateAmt | Amount-update of a prior restraint (`FIUpdateAmtRq`) |
| Requester (`Rqstr`) | The entity that requested the action (court/gov). Identified by `Rqstr/PID`. **Not** the submitting bank (header `PID`). |
| Customer / Non-customer | Whether the target party/account belongs to this bank |
| Restraint family | Block/Garnish/BanDealing/DenyDealing/Lift group; drives ANTS fan-out |
| Registry | Decision cache table keyed by a derived `LOOKUP_KEY` |
| Backend | Which DB served/serves a transaction: `DB2` (primary) or `MSSQL` (DR) |
| ANTS/SISL | Internal compliance service maintaining the block-list |

---

## 3. Target architecture (service decomposition)

Reproduce the six logical components as Spring Boot services (or modules within
a modular monolith — deployment shape is the team's choice; the boundaries and
contracts below are what matter).

| # | Service | Responsibility | Inbound | Outbound |
|---|---|---|---|---|
| 1 | **proxy-api** | HTTPS ingress; parse into the generic `Request<T>` (`MsgHdrRq` header + typed body); extract all header fields (§7.1.2); **synchronous SOAP ACK** (`MsgHdrRs`, §7.1.3); enqueue request | HTTPS POST (SOAP/XML) | queue → orchestrator; SOAP ACK |
| 2 | **relation-orchestrator** | Classification engine: registry short-circuits, per-op bypass, bank-relation check, routing fan-out, registry persistence, backend/DR decision hand-off | queue (SOAP) + registry DB | queue → transformation / callback / ants |
| 3 | **transformation** | SAMA→bank canonical transform; **backend (DB2/MSSQL-DR) routing**; reply-to QM selection | queue (SOAP) + registry DB | queue → old Tanfeeth backends |
| 4 | **nobankrel-callback** | Send SAMA `CallBackRq` for non-customers; retry ladder | queue (+context) | HTTPS → SAMA; retry/BOQ queues |
| 5 | **ants-compliance** | Add/Remove SISL block-list records; audit | queue (+context) | HTTPS → SISL; audit DB; retry/BOQ |
| 6 | **retry-dispatcher** | Generic timer-driven retry/BOQ dispatcher | retry queue (timer) | target queue / BOQ |

**IIB/ACE → Spring mapping (reference):**

| IIB/ACE | Spring Boot |
|---|---|
| MQ Input/Output node | `@JmsListener` (Spring JMS) + `JmsTemplate` to IBM MQ, or Kafka if messaging is being replaced |
| MQRFH2 `usr` folder | JMS message properties / a headers map |
| ESQL Compute module | `@Service` method |
| UDP (`DECLARE … EXTERNAL`) | `application.yml` property bound via `@ConfigurationProperties` |
| `PASSTHRU`/DB access | `JdbcTemplate` / Spring Data JPA |
| `SHARED ROW` cache (config) | Spring `@Cacheable` / `Caffeine`, refreshed on interval |
| HTTP Request node | `RestClient` / `WebClient` |
| RouteToLabel | a strategy/branch in code |
| TimeoutNotification | `@Scheduled` |
| Backout queue (BOTHRESH) | DLQ / error queue + max-delivery config |

---

## 4. Business requirements

Numbered, testable. "MUST" = mandatory.

**BR-1 — Synchronous acknowledgement.** On receiving a SAMA request the system
MUST return a synchronous SOAP acknowledgement immediately (status `S1000000`
on accept; `E9999999` on internal error), independent of downstream processing.

**BR-2 — Customer classification.** For each request the system MUST determine
whether the target is a **bank customer** using, in priority order:
1. **Per-operation bypass** — if the operation is in the configured bypass list,
   treat as customer with **no** DB or bank-relation call.
2. **Both-identifiers rule** — if the request carries **both** a party id and an
   account number, treat as customer (no external call needed).
3. **Registry short-circuit** — for reverse/lift/override/update-amount, reuse
   the prior decision from the registry (see BR-4).
4. **Bank-relation service** — otherwise call the core-banking relation service
   (request/reply) to classify.
5. **Fail-open default** — if the registry or relation service is unavailable,
   default to **customer** (never block the customer flow on infra failure).

**BR-3 — Routing by classification.**
- **Customer / bypass** → forward the original SAMA message to the existing
  Tanfeeth service.
- **Non-customer** → (a) do **not** forward to old Tanfeeth; (b) trigger the SAMA
  **callback** (auto-response), except on reverse/update-amount; (c) for
  **restraint-family** operations, publish to the **ANTS compliance** channel
  with an ADD/REMOVE action.

**BR-4 — Registry (decision memory).** The system MUST persist forward-path
classification decisions keyed by a derived `LOOKUP_KEY`, and resolve follow-up
messages against it:
- **Reverse** (`Mod<0`) — undo: soft-delete the matched active row.
- **Lift** (op in lift list) — undo: soft-delete the matched active row.
- **Override** (`OvrdMsgUID` present) — chain-promote: soft-delete the matched
  row and insert a **new active** row whose `ORIGINAL_MSGUID` = this override's
  MsgUID (so the next override finds it). Overrides are **customer-only**.
- **UpdateAmt** — look up by the **prior SRN carried in the body** (`PrvSRN`):
  miss or DB-down → forward to old Tanfeeth as-is; customer hit → forward;
  **non-customer hit** → soft-delete the matched row and drive an ANTS **REMOVE**
  keyed on the **original** record's SRN.
- **Uniqueness:** at most **one active row per `LOOKUP_KEY`** (enforced).
- **Soft-delete:** history retained (`ACTIVE_FLAG='N'`, `DELETED_TS`), never hard-deleted inline.

**BR-5 — Restraint compliance (ANTS).** For restraint-family operations on
non-customers, the system MUST maintain an external block-list: **ADD** on
forward, **REMOVE** on reverse/lift/update-amount. Each ANTS call MUST be keyed
on a **ReferenceNumber stable across the ADD/REMOVE pair** (the original SRN).
An audit row MUST be written per ANTS attempt.

**BR-6 — Callback success semantics.** A SAMA callback is deemed **successful**
when HTTP is 2xx AND the SOAP status is in the configured success set
(default: `S1000000`, `E1020025` (duplicate), `E1020026` (already processed)).
Otherwise it retries within a budget, then escalates to a manual-review queue.

**BR-7 — Idempotent retry with backoff.** Failed callback/ANTS calls MUST be
retried up to a configured max attempts via a shared retry channel drained by a
dispatcher; on exhaustion or invalid routing they escalate to a backout queue
with a stamped reason. Retries MUST NOT duplicate side effects.

**BR-8 — Backend routing (DB2 primary / MSSQL DR).** In the transformation
stage, the system MUST select the backend DB per request:
1. **Act-on-existing** (lift/reverse/override) → the backend that **served the
   original**, read from registry `SERVED_BY`. If not found, do **not** guess —
   route to error/quarantine.
2. **Fresh + DR partner** (requester `Rqstr/PID` in the DR list) → **MSSQL (DR)**.
3. **Fresh + non-DR** → **DB2 (primary)**.
The chosen backend selects the destination queue (base for DB2; explicit
`drQueue` or base+`.DR` for MSSQL) and stamps `SERVED_BY` on the fresh path.

**BR-9 — Reply-to queue manager per site.** When forwarding to old Tanfeeth, the
outgoing message MUST carry the **Tanfeeth reply-to queue manager for the target
site** (primary vs DR), NOT the interceptor's own QM. Selection follows the
`isDR` flag; both QM names are per-site configuration.

**BR-10 — Quarantine.** Unknown operations and unrecoverable transform errors
MUST be routed to a quarantine channel with a diagnostic envelope; they MUST NOT
be dropped.

**BR-11 — No blocking on infrastructure.** Registry/audit DB failures MUST be
non-blocking (log + degrade); they MUST NOT fail the customer-facing flow.

**BR-12 — Full audit & correlation.** Every message MUST be logged with
correlation id (`MsgUID`) and transaction id (`SRN`); every DB write carries a
`CREATED_BY`/`UPDATED_BY` actor and timestamps.

---

## 5. Core decision logic (normative pseudo-code)

This is the heart of the interceptor. Reproduce the **evaluation order exactly**.

### 5.1 Orchestrator — classification & registry

```
extract SAMA header: msgUid, srn, pid(bank), mod, ovrdMsgUID, operation, operationNs
extract body: accountNumber, partyId, requesterPid (Rqstr/PID)
checkScope = BOTH | ACCOUNT_ONLY | PARTY_ONLY   (from which identifiers present)

# --- per-operation bypass (highest priority) ---
if operation in OperationsBypassBankCheck:
    isCustomer = true; cacheSource = BYPASS
    route to old-Tanfeeth (customer); DO NOT read/write registry; return

if not registryEnabled: route to bank-relation call; return

isReverse   = (mod < 0)
isLift      = operation in LiftOperations
isOverride  = (len(ovrdMsgUID) > 0)
isUpdateAmt = operation in UpdateAmtOperations

if not (isReverse or isLift or isOverride or isUpdateAmt):
    # fresh forward → classify (both-ids bypass else bank-relation call)
    route to bank-relation call; return

# resolve the ORIGINAL record key
refSrn = (isLift ? body Outline/SrvcRefInfo/<LiftRefSrnElements>
        : isUpdateAmt ? body Outline/<UpdateAmtRefSrnElements e.g. PrvSRN>
        : srn)                       # reverse: SAMA retransmits original SRN
effectiveSrn = refSrn or srn
family = OperationFamilyMap[operation]  (default RESTRICT for restraint ops)
lookupKey = lower(account | party | family | effectiveSrn)

# registry lookup (atomic, fail-open):
hit = SELECT ... WHERE LOOKUP_KEY = lookupKey            # Strategy A
   or (isOverride)  WHERE ORIGINAL_MSGUID = ovrdMsgUID   # Strategy B
   or (isUpdateAmt) WHERE ORIGINAL_SRN = effectiveSrn AND ACTIVE_FLAG='Y'  # Strategy C

# UpdateAmt decision (before reverse/lift/override DML):
if isUpdateAmt:
    if miss or db-down: route to old-Tanfeeth as-is; return          # fail-open
    if hit.IS_CUSTOMER = 'Y': route to old-Tanfeeth as-is; return
    # non-customer hit:
    soft-delete matched row
    publish original SRN (hit.ORIGINAL_SRN) for the ANTS REMOVE
    isCustomer=false; drive ANTS REMOVE; return

if hit:
    isCustomer = hit.IS_CUSTOMER
    if isReverse or isLift: soft-delete matched row
    if isOverride:          soft-delete matched row; INSERT new active row
                            (ORIGINAL_MSGUID = this msgUid; inherit key/scope/cust/srn)
else:
    isCustomer = true; cacheSource = DEFAULT_BANK        # fail-open default
```

### 5.2 Orchestrator — response routing (after classification)

```
if isCustomer: forward to old-Tanfeeth (transformation channel)
else:
    complianceAction = (isLift or isReverse or isUpdateAmt) ? REMOVE : ADD
    if not (isReverse or isUpdateAmt): send SAMA callback (non-customer channel)
    if family in RestraintFamilies:    publish to ANTS channel with complianceAction
    if no destination resolved:        consume (nothing to do)

# on the forward (fresh, customer) path only:
if writing a new decision (not a cache hit / bypass / update-amt):
    upsert registry: soft-delete existing active by LOOKUP_KEY; INSERT new active row
# hand-off to transformation for backend routing:
set usr.actOnExisting, usr.origKeyCol, usr.origKeyVal   (for lift/reverse/override)
```

### 5.3 Transformation — backend (DB2/MSSQL-DR) routing

```
if usr.actOnExisting == 'Y':
    backend = SELECT SERVED_BY FROM registry
              WHERE <usr.origKeyCol> = usr.origKeyVal AND SERVED_BY IS NOT NULL
              ORDER BY CREATED_TS DESC          # ignore ACTIVE_FLAG (may be soft-deleted)
    if backend not in {DB2, MSSQL}: throw → quarantine       # never guess
    isDR = (backend == MSSQL)
elif requesterPid in drPartners:  backend=MSSQL; isDR=true
else:                             backend=DB2;   isDR=false

targetQueue = configEntry(operation).queue        # by operation name; default = quarantine
if isDR: targetQueue = configEntry.drQueue ?? (targetQueue + ".DR")
if targetQueue is empty: throw with operation name # prevents MQ 2085

if not actOnExisting:  UPDATE registry SET SERVED_BY = backend
                        WHERE ORIGINAL_SRN = srn AND ACTIVE_FLAG='Y'   # non-blocking

replyToQMgr = isDR ? TanfeethReplyToQMgrDr : TanfeethReplyToQMgrPrimary
serviceCode = configEntry.serviceCode              # from the SAME entry as queue
route message to targetQueue with reply-to = replyToQMgr
```

---

## 6. Data model — relation registry

Single table (present on both DB2 and MSSQL; same structure/keys).

| Column | Type | Notes |
|---|---|---|
| REGISTRY_ID | BIGINT identity | surrogate PK |
| LOOKUP_KEY | varchar(128) | `lower(account\|party\|family\|srn)` |
| OPERATION | varchar(64) | SAMA op (audit) |
| OPERATION_FAMILY | varchar(32) | drives LOOKUP_KEY + restraint gate |
| ACCOUNT_NUMBER | varchar(34) | nullable |
| PARTY_ID | varchar(50) | nullable |
| CHECK_SCOPE | varchar(16) | BOTH / ACCOUNT_ONLY / PARTY_ONLY |
| IS_CUSTOMER | char(1) | Y / N |
| ORIGINAL_MSGUID | varchar(100) | override chain key |
| ORIGINAL_SRN | varchar(50) | reverse/lift/update-amt key |
| ACTIVE_FLAG | char(1) | Y / N (soft delete) |
| CREATED_BY / CREATED_TS | varchar(100) / ts | actor + time |
| UPDATED_BY / UPDATED_TS | varchar(100) / ts | |
| DELETED_TS | ts | set on soft-delete |
| SERVED_BY | varchar(10) | DB2 / MSSQL (backend that served it) |

**Constraints/indexes (MUST):**
- **Unique active key**: at most one row with `ACTIVE_FLAG='Y'` per `LOOKUP_KEY`
  (filtered unique index on MSSQL; `EXCLUDE NULL KEYS` generated-column index on
  DB2). The Java layer MUST treat a unique-violation on this as "another active
  row exists" and handle per the upsert semantics.
- Indexes on `ORIGINAL_MSGUID+ACTIVE_FLAG`, `PARTY_ID+ACCOUNT_NUMBER+FAMILY+ACTIVE_FLAG`, `CREATED_TS`, `DELETED_TS`.

**Second table** — ANTS block-list audit log (ADD/REMOVE trail); one row per attempt.

---

## 7. Integration contracts

### 7.1 Inbound (SAMA → proxy-api)

- **Transport:** HTTPS POST, content-type `application/xml`, SOAP 1.1 envelope.
- **Namespaces:** header `head = http://www.sama.bea.sa/common/Header`;
  base lib `bas = http://www.sama.bea.sa/common/BaseLib`; each operation body
  has its own service namespace (namespace-agnostic matching by local-name).

#### 7.1.1 Generic request model — `Request<T>`

Every inbound message is the **same envelope shape**: a common `MsgHdrRq`
header plus one operation body. Model it as a **generic type** parameterised by
the request body:

```java
// envelope: <soapenv:Envelope><soapenv:Header><MsgHdrRq/></Header>
//           <soapenv:Body><T/></soapenv:Body></soapenv:Envelope>
class SamaRequest<T extends SamaBody> {
    MsgHdrRq header;   // always present, common to all operations
    T        body;     // the typed operation payload (FIBlockRq, FIDeductionRq, …)
}
class SamaResponse {          // ACK only carries a header + empty op element
    MsgHdrRs header;
    String   operationLocalName;   // echoed empty body element
}
```

- `SamaBody` is a marker for operation payloads; `body`'s XML local-name **is**
  the operation (used for routing/config lookup). Bind concrete bodies with JAXB
  and dispatch by local-name, or keep the body as a DOM/`XmlNode` and read paths
  generically (the interceptor mostly needs the header + a few body paths, not a
  full binding of every operation).

#### 7.1.2 `MsgHdrRq` — request header fields (extract all)

| Field | Req? | Used for |
|---|---|---|
| `PID` | required | submitting **bank** id (NOT the requester/DR partner) |
| `Sys` | optional | source system |
| `MsgDtTm` | required | message datetime (echoed in ACK) |
| `ChID` | optional | channel id |
| `MsgUID` | required | **correlation id**; also `CRN` in the ACK |
| `OvrdMsgUID` | optional | **override signal** — present ⇒ override; = the overridden MsgUID |
| `SRN` | optional | **transaction id**; registry key for reverse |
| `Cnfd` | optional | confidentiality flag |
| `Mod` | optional | modifier; **`Mod < 0` ⇒ reversal** |
| `CRN` | optional | case reference |
| `Clsf` | optional | classification (drives ANTS source mapping) |
| `pHash` | optional | payload hash |
| `IPAdrs` | optional | source IP |
| `Status` | optional | status |
| `Note` | optional | free text |
| `Info` | optional | **repeating** `bas:Attribute { Name, Value }` list |

Map to a `MsgHdrRq` POJO; `Info` → `List<Attribute>` where
`Attribute { String name; String value; }`.

#### 7.1.3 `MsgHdrRs` — ACK response header (build)

| Field | Source |
|---|---|
| `PID` | echo request `PID` |
| `Status` | `S1000000` (accepted) / `E9999999` (internal error) |
| `MsgDtTm` | echo request `MsgDtTm` |
| `SRN` | echo request `SRN` (optional) |
| `CRN` | request **`MsgUID`** |
| `Info` | optional repeating `bas:Attribute { Name, Value }` |

The ACK body is an **empty element of the same operation local-name** in the
operation's namespace. Response `Request` type: `SamaResponse` (header + op name).

#### 7.1.4 Body identifier extraction

Account and party live in operation-specific body paths, e.g. deduction account
at `Outline/DeductionInfo/ExecDeduction/AccId/Num`; party at
`InvPrty/<type>/Id`; requester at `Rqstr/PID`. The rebuild MUST implement a
**resilient extractor**: try the known explicit paths, then a **recursive
descendant fallback** for the first `AccId/Num` (never mistaking a beneficiary
`Benf/IBAN` for the debit account) and for `InvPrty/<type>/Id`.

### 7.2 Internal messaging (contract, not transport-specific)
Preserve these logical channels and the context they carry (as message
properties/headers): `operation`, `operationNs`, `checkScope`, `msgUid`, `srn`,
and for follow-ups `actOnExisting`, `origKeyCol`, `origKeyVal`; for ANTS
`complianceAction`; for retry `targetQueue`, `sourceApp`, `maxAttempts`,
`retryCount`, `correlationId`, `lastHttpStatus`, `lastSoapStatus`, `boqReason`.

| Logical channel | From → To |
|---|---|
| guard-request | proxy → orchestrator |
| acc-check req/reply | orchestrator ↔ bank-relation responder (request/reply) |
| internal-map (old Tanfeeth) | orchestrator → transformation |
| no-relation (callback) | orchestrator → callback |
| internal-block-list | orchestrator → ants |
| retry / boq | callback+ants → retry-dispatcher → target/boq |
| transform outputs | transformation → per-op backend gateways / quarantine |

### 7.3 Outbound HTTP
- **Bank-relation service:** POST JSON `{ identifier: { idNumber, idType, accountNumber, ibanNumber } }` with client-id/secret headers; response yields `belongsToBank`/`isCustomer`.
- **SAMA callback (`CallBackRq`):** per-operation SOAP built from a JSON config catalogue (endpoint, namespace, per-checkScope status codes & extra body elements).
- **SISL/ANTS:** SOAP `AddSAMARecord` / `RemoveSAMARecord` with credentials; ReferenceNumber = stable original SRN.

---

## 8. Configuration (externalize as `application.yml` / config service)

All of the following are runtime-configurable (were IIB UDPs). No hardcoding.

| Key | Meaning | Example default |
|---|---|---|
| registry.enabled | registry feature flag | true |
| registry.datasource / schema | DB connection + schema | TANFEETHEXEC |
| operationFamilyMap | op → family (CSV/map) | restraint ops → RESTRICT |
| liftOperations | ops treated as lift | FILiftRq |
| liftRefSrnElements | body element names for lift ref-SRN | SRN,SnetRN |
| updateAmtOperations | ops treated as update-amount | UpdateAmtRq,FIUpdateAmtRq |
| updateAmtRefSrnElements | body element for prior SRN | PrvSRN |
| operationsBypassBankCheck | ops that bypass classification | (empty) |
| restraintFamilies | families that fan out to ANTS | RESTRICT,BLOCK,BANDLNG,GARNISH |
| callback.config | per-op callback catalogue (JSON) | — |
| callback.successStatusCodes | SOAP success set | S1000000,E1020025,E1020026 |
| callback.maxAttempts | retry budget | 3 |
| ants.endpoint / credentials / maps | SISL config | — |
| retry.allowedTargets / pollInterval / maxPerTick | dispatcher | — |
| routing.serviceConfig | per-op {branch, queue, serviceCode, drQueue?} + drPartners[] | — |
| routing.replyToQMgrPrimary / replyToQMgrDr | per-site reply QMs | — |
| bankRelation.endpoint / clientId / clientSecret / timeout | relation service | — |

---

## 9. Error handling, retry, resilience

- **DB access** MUST be wrapped so failures are non-blocking (log with SQL
  diagnostics; classification falls open to customer). Use short timeouts.
- **Callback/ANTS** follow the retry ladder: success (BR-6) → done; failure with
  budget → retry channel with incremented `retryCount`; exhausted/invalid →
  backout with `boqReason`.
- **Retry dispatcher** rules (first match): missing target → BOQ; target == retry
  channel (loop) → BOQ; target not in allow-list → BOQ; `retryCount ≥ max` → BOQ;
  else dispatch to target. Timer-driven; per-tick throughput cap.
- **Poison messages**: max-delivery / backout threshold → DLQ (equivalent of the
  IIB BOQ per input queue).
- **Idempotency**: registry writes use unique-active-key + soft-delete;
  reprocessing the same message MUST NOT create duplicates or double side effects.

---

## 10. Non-functional requirements

- **Throughput/volume:** high-volume financial traffic; registry may hold
  100M+ rows historically — queries MUST be index-driven; classification path
  MUST avoid table scans.
- **Latency:** the synchronous ACK (BR-1) MUST return promptly regardless of
  downstream load (decouple via async messaging).
- **Availability / DR:** primary (DB2) and DR (MSSQL) backends; the interceptor
  runs at both sites; backend + reply-QM selection are the DR switch (BR-8/9).
- **Security:** TLS throughout; secrets (client id/secret, SISL creds) from a
  vault, never in source; PII (party ids, account numbers, names incl. Arabic —
  use full Unicode/NVARCHAR) handled per bank policy.
- **Observability:** structured logs with `MsgUID`/`SRN`; per-decision log line
  `backend=… isDR=… actOnExisting=… targetQueue=…`; metrics on classification
  outcomes, callback success rate, retry/BOQ counts.
- **Config hot-reload:** routing config / partner lists refresh on an interval
  (≈30s) without redeploy.

---

## 11. Acceptance criteria (behavioural, must all pass)

1. Fresh non-customer → callback fired, no forward to old Tanfeeth, registry row
   written with `IS_CUSTOMER='N'`.
2. Fresh customer (or both-ids / bypass) → forwarded to old Tanfeeth; correct
   backend (DB2 default; MSSQL if DR partner) and reply-to QM; `SERVED_BY` stamped.
3. Reverse/Lift of a prior record → matched row soft-deleted; routed to the
   **same backend** as the original; ANTS **REMOVE** for restraint families.
4. Override → chain-promote (old soft-deleted, new active with this MsgUID as
   `ORIGINAL_MSGUID`); customer-only.
5. UpdateAmt: miss/customer → forward as-is; non-customer hit → ANTS REMOVE keyed
   on the **original** SRN.
6. Callback success accepted for `S1000000/E1020025/E1020026`; others retry then
   BOQ after max attempts.
7. Backend not found for an act-on-existing message → quarantine, never a guessed
   backend.
8. Unknown op / transform error → quarantine with diagnostics.
9. Registry/DB outage → customer flow still succeeds (fail-open); warnings logged.
10. At most one active registry row per `LOOKUP_KEY` at all times.

---

## 12. Suggested Spring Boot design (non-binding)

- **Modules:** `proxy-api` (WebFlux/MVC + JAXB/Spring-WS for SOAP), shared
  `sama-model` (the generic `SamaRequest<T>` / `MsgHdrRq` / `MsgHdrRs` /
  `Attribute` POJOs + JAXB bindings + the resilient body extractors),
  `orchestrator`, `transformation`,
  `callback`, `ants`, `retry-dispatcher`, `registry` (Spring Data JPA + Flyway).
- **Messaging:** Spring JMS over IBM MQ to preserve the MQ estate, or Kafka if
  messaging is being modernised — keep the logical channels + context headers.
- **SOAP:** Spring-WS or JAXB + `RestClient`; strict schema handling; namespace-
  aware body extractors with the recursive fallback (§7.1).
- **Registry:** JPA entity + a repository with explicit upsert (insert; on
  unique-active-key violation, soft-delete existing then insert) inside a
  `@Transactional` with a short timeout; a fail-open wrapper that catches
  `DataAccessException` and returns the customer default.
- **Config:** `@ConfigurationProperties` + `@RefreshScope` (Spring Cloud Config)
  or a Caffeine-cached config bean refreshed every 30s.
- **Resilience:** Resilience4j (timeouts, retries, circuit breakers) on the
  bank-relation, callback, and ANTS HTTP clients.
- **Scheduling:** `@Scheduled` for the retry dispatcher tick.
- **Testing:** contract tests for each channel's headers; Testcontainers for
  DB2+MSSQL registry parity; WireMock for SAMA/SISL/bank-relation; the §11
  acceptance criteria as end-to-end scenarios.

---

## 13. Open items the agent MUST confirm (do not invent)

- Exact SAMA operation → family mapping and the full operation catalogue.
- Bank-relation service request/response schema and auth.
- Callback catalogue (endpoints, per-op body shapes) per environment.
- SISL/ANTS WSDL and the ReferenceNumber/entity rules.
- DR partner list source (config vs table) and the exact requester field
  (`Rqstr/PID` vs `Rqstr/RID`).
- Queue/topic names and the messaging platform (keep MQ vs move to Kafka).
- Whether messaging is retained (IBM MQ) or replaced; if replaced, re-map the
  MQRFH2/BOQ semantics to the new platform.
```
