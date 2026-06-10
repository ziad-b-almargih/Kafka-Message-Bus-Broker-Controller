# Smart Package Processing System using Apache Kafka & .NET

A complete Event-Driven Architecture (EDA) project built using `.NET`, `Apache Kafka`, and `Docker Compose`. This system simulates a logistics pipeline designed to receive, process, validate, and isolate messages securely, ensuring zero data loss even when processing faulty payloads.

---

## 📌 Project Overview

The architecture consists of three distinct microservices running concurrently and communicating asynchronously via Kafka:

* **Producer Service (Port 7001):** A web console that allows operators to dispatch custom payloads or quick-action packages (valid packages or corrupt packages tagged with `:fail`) into the primary processing lane.
* **Worker/Consumer Service (Port 7002):** The core engine that continuously consumes incoming packages, executes validation logic, implements retry mechanisms, and dispatches persistent failures to quarantine.
* **Dead Letter Queue (DLQ) Service (Port 7003):** A specialized quarantine vault that consumes heavily corrupted or "poison" payloads, exposing them on a secure interface for manual engineering review without blocking the main production pipeline.

---

## 🧠 Core Software & Architectural Concepts Applied

### 1. Dead Letter Queue (DLQ) Pattern
To prevent "Poison Pill" scenarios—where a single corrupted message repeatedly crashes a consumer and completely halts a queue—the system isolates unprocessable payloads. These messages are redirected to a dedicated Kafka topic (`warehouse-dead-letter-queue`), allowing the main pipeline to continue processing subsequent valid traffic seamlessly.

### 2. Resilient Retry Policies
When a business logic transaction fails, the Worker service does not instantly discard the message. It executes an in-memory retry loop (up to 3 attempts) interspersed with a 1-second delay, offering transient network drops or database locks a window to self-correct.

### 3. Manual Offset Commits
Automatic commits are explicitly disabled (`EnableAutoCommit = false`). The consumer only commits the transaction offset to the broker after the payload is safely processed by the worker logic or successfully written into the DLQ. This approach guarantees **at-least-once delivery** and eliminates data drop risks.

### 4. Init Container Pattern
Kafka brokers take time to boot and initialize metadata. To bypass synchronization failures and metadata caching lags where consumers fail to find runtime-created topics, a lightweight initialization container handles topic deployment. It verifies broker health and provisions topics prior to spinning up the .NET applications.

### 5. Non-Blocking Background Tasks
Utilizing `await Task.Yield();` at the beginning of the `BackgroundService` execution thread releases the main initialization thread back to ASP.NET Core. This allows Kestrel (the web server) to spin up interfaces on designated ports simultaneously while Kafka consumers execute long-polling operations in the background without resource starvation.

---

## 🛠️ System Dependencies

* **Runtime Environment:** `.NET 8.0 SDK` (or later) for background workers and Web APIs.
* **Message Broker:** `Apache Kafka` (Official Apache Image).
* **Client Library:** `Confluent.Kafka` for internal low-level Producer/Consumer bindings.
* **Orchestration:** `Docker` & `Docker Compose` for multi-container deployment and network isolation.

---

## 🚀 Execution & Deployment Guide

The entire multi-service mesh can be built and deployed natively using a single orchestrated command. There is no need to manually pre-configure topics or download binary dependencies locally.

1. Open a terminal instance inside the root directory containing your `docker-compose.yml` file.
2. Execute the following command to completely purge legacy volumes, rebuild application layers, and start the system in detached mode:

```bash
sudo docker compose down -v && sudo docker compose up --build -d
```

### Infrastructure Lifecycle Steps
1. The engine starts the primary `kafka-server` instance and monitors its built-in health check.
2. Once healthy, the `kafka-init` worker mounts, creates `warehouse-processing-lane` (3 partitions) and `warehouse-dead-letter-queue` (1 partition), and exits cleanly.
3. The three C# services initialize concurrently and hook directly into the prepared topics.

---

## 📊 Management Dashboards

Once operational, individual web portals can be accessed locally to interact with and observe the event pipeline in real time:

* **Producer Console:** 🔗 [http://localhost:7001](http://localhost:7001)  
    *Use this interface to input custom message streams or invoke quick actions to generate pseudo-randomized traffic.*
    
* **Worker Operations:** 🔗 [http://localhost:7002](http://localhost:7002)  
    *Monitor active polling cycles, real-time message evaluations, and retry loops on failed processing blocks.*
    
* **DLQ Isolation Vault:** 🔗 [http://localhost:7003](http://localhost:7003)  
    *View quarantined payloads that failed all internal validation and retry thresholds.*
