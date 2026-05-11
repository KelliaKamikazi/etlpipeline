# WHO Health ETL Pipeline

A Python pipeline that pulls adolescent birth rate data from the WHO Global Health Observatory API, cleans it up, and stores it in a PostgreSQL database.

## What You Need

- Python 3.11 or newer
- PostgreSQL installed and running on your machine

## Getting Started

1. Create the database:

```sql
CREATE DATABASE who_health;
```

2. Set up the tables:

```bash
psql -U postgres -d who_health -f schema.sql
```

3. Copy the example environment file and update the values if needed (database password, etc.):

```bash
cp .env.example .env
```

4. Install the required Python packages:

```bash
pip install -r requirements.txt
```

## What Data Does It Pull?

The pipeline fetches adolescent birth rate statistics from the WHO API, but only for five countries: **Rwanda, Singapore, Spain, USA, and Mexico**. These were picked randomly just to keep things small and avoid hammering the API. You can change which countries are included by editing the `TARGET_COUNTRIES` set in `etl.py`.

## Running the Pipeline

```bash
python etl.py
```

### Stopping and Resuming

The pipeline saves its progress after every batch of records it processes. This means:

- If it gets interrupted for any reason (you hit Ctrl-C, your machine crashes, the network drops), just run it again. It will pick up where it left off instead of starting over.
- If it already finished successfully, running it again will only check for new data on the API. If nothing new is there, it exits right away without re-downloading everything.

If you want to start completely fresh and re-download all the data:

```bash
python etl.py --full-refresh
```

### Why Is This Safe?

Each batch of data and its progress marker are saved to the database together in a single transaction. So the pipeline always knows exactly what has been loaded. If something fails halfway through a batch, that batch is rolled back entirely and will be retried on the next run.

## What I Would Improve With More Time

- **Split into separate files.** Right now all the code lives in one file. I would break it up into smaller modules (extract, transform, load) so each piece is easier to read, test, and work on independently.
- **Docker setup.** I would add Docker so anyone can run the pipeline and the database with one command, without needing to install Postgres on their machine.

## How I Would Test and Debug This

- For the **transform logic**, I would create fake records (some valid, some broken, some for countries not in the list) and check that the function handles each case correctly.
- For the **database loading**, I ran the pipeline against a test database and queried the tables to verify the right data ended up in the right place — it works as expected.
- For **debugging**, the pipeline already logs how many records passed or were skipped in each batch. If something looks wrong, I would start there, then look at the specific validation errors to figure out which records failed and why. For database issues, I would connect directly with psql and inspect the data.
