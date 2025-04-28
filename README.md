#### **Real-Time CDR Processing Pipeline**

This project is designed for providing a base structure, not for a completed pipeline.

---

### **Use Case Overview**

Telecom companies generate massive amounts of **Call Data Records (CDRs)** from phone calls, SMS, and internet usage. These records need to be processed in real time for **billing, fraud detection, and network optimization**. This project enables a **real-time streaming pipeline** to process, analyze, and store CDR data for better telecom operations.

---

### **How the Project Works**

1. **CDR Ingestion Pipeline (`ingestion.py`)**
   - Reads real-time CDR data from Kafka (`cdr_raw_topic`).
   - Cleans and validates records (e.g., removing incomplete or duplicate CDRs).
   - Stores cleaned CDR data into HBase via Phoenix.
   - Publishes an event message (`call_id`, `timestamp`) to another Kafka queue (`cdr_events_topic`).
2. **CDR Processing & Analysis Pipeline (`transform.py`)**
   - Reads CDR event messages from Kafka.
   - Fetches full CDR details from HBase.
   - Performs **fraud detection** (e.g., SIM-box fraud, unusual call spikes).
   - Computes **real-time billing charges** based on call duration and tariff plans.
   - Joins with customer profiles to track usage trends.
   - Publishes **billing and fraud alert events** to Kafka (`billing_events_topic`, `fraud_alerts_topic`).
3. **Final Storage & Reporting Pipeline (`load.py`)**
   - Reads `billing_events_topic` and `fraud_alerts_topic` from Kafka.
   - Stores final processed billing and fraud data into HBase (`cdr_billing_table`, `cdr_fraud_table`).
   - Sends real-time fraud alerts to monitoring systems.

---

### **Key Benefits**

- **Real-time billing**: Accurately charge customers based on usage without delays.
- **Fraud detection**: Identify suspicious activities (e.g., SIM box fraud, unusual roaming patterns).
- **Network insights**: Analyze call patterns to optimize network performance.
- **Scalability**: Handle millions of CDRs per second across multiple telecom regions.
