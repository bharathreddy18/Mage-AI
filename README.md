# IDP-Mage Data Pipelines

This repository contains the data engineering pipelines for the Integrated Data Portal (IDP). This pipelines leverage **Mage-AI** to manage, transform, and export state-specific data (such as Agmarknet) to Wasabi cloud storage.

The project is structured to run inside a Dockerized environment to ensure consistency across different developer machines.

---

## Getting Started

Follow these steps to set up your local environment and spin up the data factory.

### 1. Local Python Setup (Optional but Recommended)
You can clone this repository into your local machine.

**Go to your convinient folder in your local and then Clone and experiment with it:**
```powershell
# Create the repo
git clone https://github.com/bharathreddy18/Mage-AI.git

```

Setting up a local virtual environment helps with code completion and linting in VS Code.

**Create and Activate Virtual Environment:**
```powershell
# Create the venv
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate.ps1

# Activate (Mac/Linux)
source .venv/bin/activate

```

**Install Requirements:**

```bash
pip install -r requirements.txt

```

---

### 1. Environment Configuration

Create a `.env` file in the root directory (`Mage-AI/`) and fill in your credentials:

```env
WASABI_BUCKET=your_bucket_name
WASABI_PREFIX=your_folder_prefix
WASABI_ACCESS_KEY=your_access_key
WASABI_SECRET_ACCESS_KEY=your_secret_key
WASABI_ENDPOINT=[https://s3.wasabisys.com](https://s3.wasabisys.com)

```

### 2. Build and Launch

Run the following commands to create the images and start the container:

```bash
# Stop any existing containers
docker compose down

# Build the image from scratch (ignoring broken cache)
docker compose build --no-cache

# Start the container in the background
docker compose up -d

```

---

## Mage-AI Operations

### 1. Access the Dashboard

Once the container is healthy, open your browser to:
**[http://localhost:6789](https://www.google.com/search?q=http://localhost:6789)**

### 2. Project Structure

* **Data Loaders**: Python scripts for fetching raw data.
* **Transformers**: Logic for cleaning and converting data to Parquet.
* **Data Exporters**: Final step to upload files to Wasabi and update the Codebook.
* **Utils**: Shared helper functions (e.g., Wasabi connection, date windowing).

### 3. Monitoring and Logs

To see what is happening inside the container (errors, print statements, etc.):

```bash
docker compose logs -f server

```

---

## Triggers and Automation

Mage-AI uses **Triggers** to automate data retrieval.

1. **Schedules**: You can set the pipeline to run Daily or Weekly.
2. **Backfills**: If data for a previous week is missing, use the "Backfill" feature to run the pipeline for specific historical dates.
3. **Variables**: Use `kwargs.get('execution_date')` in your blocks to make your logic dynamic based on the trigger time.

---

**Developed for the Integrated Data Portal (IDP).**

```
