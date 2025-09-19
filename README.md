# Healthcare No-Show ETL Project

Overview
This project demonstrates an **object-oriented ETL pipeline** that extracts, transforms, and loads healthcare data to analyze patient appointment no-shows.  
It uses **synthetic data** generated on the fly and served through a **Flask API**.  

**Main outputs:**
- `patient_no_show_summary.csv` — Clean, aggregated dataset including patient names, appointment status, and reminder details.
- `cleaned_data.zip` — ZIP file containing the cleaned dataset for easy sharing.

This project is structured to follow **PEP 8 guidelines** (with a `flake8-html` report provided) and can easily be extended to production pipelines.

---

Prerequisites
- **Python 3.8+**
- **Git**
- Recommended: VS Code or similar IDE
- Installed tools: `pip`, `venv` (ships with Python)

---

Build & Activate Virtual Environment

1. Create a virtual environment
```bash
python -m venv venv

Activating virtual environment in git bash
source /env/scripts/activate


How to run the code:
Install dependencies

pip install -r requirements.txt

Step 1. running the project

python generate_healthcare_data.py

This will:

##Generate patients.csv, appointments.json, and reminders.json.

Start a Flask API at http://127.0.0.1:5000/appointments
.

Keep this terminal running so the ETL can pull data from the API.##


Step 2.: Run the ETL pipeline

python etl_pipeline.py

## This will:

Extract data from CSV, JSON, and API.

Transform: normalize dates, join patient info, compute no-show counts.

Load: save cleaned dataset to patient_no_show_summary.csv and compress into cleaned_data.zip. ##


##How to generate a Flake8-html file##

flake8 --format=html --htmldir=flake8-report

## This will create an HTML report in the flake8-report folder.
Open flake8-report/index.html in a browser to see compliance results.
You can print to PDF if a PDF report is required for submission. ##
