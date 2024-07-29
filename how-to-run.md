# How to Run this Project

## Prerequisites

- Python 3.9 or higher
- Docker and Docker Compose
- Git (optional, for cloning the repository)

## Step 1: Clone the Repository

If you haven't already, clone the project repository:

```bash
git clone https://github.com/sbm93/kafka-kong.git
cd kafka-kong
```

## Step 2: Set Up the Python Environment

Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Step 3: Configure the Project

Review and update the `config/config.yaml` file. 


## Step 4: Run Kafka and OpenSearch Services

```bash
docker-compose up -d
```

## Step 5: Run the Kafka Producer (Ingestion)

To ingest CDC events from a file into Kafka, use the following command:

```bash
python -m src.main --ingest input_data/stream.jsonl
```

## Step 6: Run the kafka consumer

To consume events from Kafka and store them to OpenSearch, use:

```bash
python -m src.main --consume
```

This process will continue running, consuming messages from Kafka and store them to OpenSearch until you stop it.

## Step 7: Verify the Results

### Kafka UI
- Access the Kafka UI at `http://localhost:8080`
- Verify that messages have been ingested into the `cdc-events` topic

### OpenSearch
- Use curl commands or access OpenSearch Dashboards at `http://localhost:5601`
- In OpenSearch Dashboards, go to "Dev Tools" or "Query Workbench"
- Run a sample query to check the ingested data:

  ```
  GET /cdc/_search
  {
    "query": {
      "match_all": {}
    }
  }
  ```

## Stopping the Project

1. Stop the kafka consumer process (Ctrl+C)
2. Stop the Docker services:
   ```bash
   docker-compose down
   ```
