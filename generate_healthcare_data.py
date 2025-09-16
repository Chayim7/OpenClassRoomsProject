# generate_healthcare_data.py
"""
OOP Data Generator + Mock API Server
- Run this script to produce:
  - patients.csv
  - appointments.json
  - reminders.json
  - start a Flask API serving appointments at /appointments
"""

import csv
import json
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from faker import Faker
from flask import Flask, jsonify
import logging

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("data_gen")

# ---------------------------
# Base Generator
# ---------------------------
class BaseDataGenerator:
    def __init__(self, seed: Optional[int] = None):
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    @staticmethod
    def random_date(start: datetime, days_range: int) -> datetime:
        return start + timedelta(days=random.randint(0, days_range))

    @staticmethod
    def parse_date_flexible(date_str: str) -> datetime:
        formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unknown date format: {date_str}")

# ---------------------------
# Patient Generator
# ---------------------------
class PatientGenerator(BaseDataGenerator):
    def __init__(self, num_patients: int = 500, seed: Optional[int] = None):
        super().__init__(seed=seed)
        self.num_patients = num_patients
        self.output_file = "patients.csv"

    def generate(self) -> List[List]:
        log.info("Generating patients...")
        rows = []
        for i in range(1, self.num_patients + 1):
            name = self.faker.name()
            age = random.randint(0, 90)
            gender = random.choice(["M", "F", "Other"])
            phone = self.faker.phone_number() if random.random() > 0.1 else ""  # missing 10%
            email = self.faker.email() if random.random() > 0.1 else ""         # missing 10%
            rows.append([i, name, age, gender, phone, email])
        self._save(rows)
        return rows

    def _save(self, rows: List[List]):
        headers = ["patient_id", "name", "age", "gender", "phone", "email"]
        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        log.info(f"Saved {len(rows)} patients to {self.output_file}")

# ---------------------------
# Appointment Generator
# ---------------------------
class AppointmentGenerator(BaseDataGenerator):
    def __init__(self, num_appointments: int = 2000, num_patients: int = 500, seed: Optional[int] = None):
        super().__init__(seed=seed)
        self.num_appointments = num_appointments
        self.num_patients = num_patients
        self.output_file = "appointments.json"
        self.doctor_names = ["Dr. Patel", "Dr. Chen", "Dr. Smith", "Dr. Lee"]

    def generate(self) -> List[Dict]:
        log.info("Generating appointments...")
        appointments = []
        base = datetime(2024, 1, 1)
        for i in range(1, self.num_appointments + 1):
            patient_id = random.randint(1, self.num_patients)
            date = self.random_date(base, 364)
            # intentionally mix formats to simulate messy input
            date_format = random.choice(["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"])
            date_str = date.strftime(date_format)
            appt = {
                "appointment_id": i,
                "patient_id": patient_id,
                "date": date_str,
                "doctor": random.choice(self.doctor_names),
                "status": random.choice(["attended", "no-show"])
            }
            appointments.append(appt)
        self._save(appointments)
        return appointments

    def _save(self, appointments: List[Dict]):
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(appointments, f, indent=2)
        log.info(f"Saved {len(appointments)} appointments to {self.output_file}")

# ---------------------------
# Reminder Generator
# ---------------------------
class ReminderGenerator(BaseDataGenerator):
    def __init__(self, appointments: List[Dict], seed: Optional[int] = None):
        super().__init__(seed=seed)
        self.appointments = appointments
        self.output_file = "reminders.json"

    def generate(self) -> List[Dict]:
        log.info("Generating reminders...")
        reminders = []
        for appt in self.appointments:
            reminder_sent = random.random() > 0.2  # 80% get reminders
            reminder_time = None
            if reminder_sent:
                # parse appointment date robustly
                appt_date = self.parse_date_flexible(appt["date"])
                reminder_time = (appt_date - timedelta(days=random.randint(1, 3))).strftime("%Y-%m-%dT%H:%M:%S")
            reminders.append({
                "appointment_id": appt["appointment_id"],
                "patient_id": appt["patient_id"],
                "reminder_sent": reminder_sent,
                "reminder_time": reminder_time
            })
        self._save(reminders)
        return reminders

    def _save(self, reminders: List[Dict]):
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(reminders, f, indent=2)
        log.info(f"Saved {len(reminders)} reminders to {self.output_file}")

# ---------------------------
# Appointment API Server (encapsulates Flask)
# ---------------------------
class AppointmentAPIServer:
    def __init__(self, appointments: List[Dict], host: str = "127.0.0.1", port: int = 5000):
        self.appointments = appointments
        self.host = host
        self.port = port
        self.app = Flask("appointment_api")
        self._configure_routes()

    def _configure_routes(self):
        @self.app.route("/appointments", methods=["GET"])
        def get_appointments():
            return jsonify(self.appointments)

    def run(self, debug: bool = False, use_reloader: bool = False):
        log.info(f"Starting Flask API at http://{self.host}:{self.port}/appointments")
        # disable reloader to avoid duplicate generation logs
        self.app.run(host=self.host, port=self.port, debug=debug, use_reloader=use_reloader)

# ---------------------------
# Orchestrator
# ---------------------------
class DataGenerationPipeline:
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        self.num_patients = 500
        self.num_appointments = 2000

    def run(self, start_api: bool = True):
        log.info("Starting data generation pipeline...")
        pg = PatientGenerator(num_patients=self.num_patients, seed=self.seed)
        patients = pg.generate()

        ag = AppointmentGenerator(num_appointments=self.num_appointments, num_patients=self.num_patients, seed=self.seed)
        appointments = ag.generate()

        rg = ReminderGenerator(appointments=appointments, seed=self.seed)
        reminders = rg.generate()

        if start_api:
            api = AppointmentAPIServer(appointments=appointments)
            # run the Flask app (blocking). Set use_reloader=False to avoid duplicate prints when debug True.
            api.run(debug=False, use_reloader=False)
        else:
            log.info("Data generation complete (API not started).")

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    pipeline = DataGenerationPipeline(seed=42)
    # Call pipeline.run() to generate files and start the API server
    pipeline.run(start_api=True)
