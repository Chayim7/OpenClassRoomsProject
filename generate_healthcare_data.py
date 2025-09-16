# generate_healthcare_data.py
import csv
import json
import random
from faker import Faker
from flask import Flask, jsonify
from datetime import datetime, timedelta


# ---------- Base Class ----------
class DataGenerator:
    def __init__(self, faker=None):
        self.fake = faker or Faker()

    def generate(self):
        """Abstract method for generating data"""
        raise NotImplementedError("Subclasses must implement generate()")

    def save_to_file(self, data, file_path, mode="w"):
        """Save data to file (csv or json based on extension)"""
        if file_path.endswith(".csv"):
            with open(file_path, mode, newline="") as f:
                writer = csv.writer(f)
                writer.writerows(data)
        elif file_path.endswith(".json"):
            with open(file_path, mode) as f:
                json.dump(data, f, indent=2)


# ---------- Subclasses ----------
class PatientGenerator(DataGenerator):
    def __init__(self, num_patients=500):
        super().__init__()
        self.num_patients = num_patients

    def generate(self):
        patients = [["patient_id", "name", "age", "gender", "phone", "email"]]
        for i in range(1, self.num_patients + 1):
            name = self.fake.name()
            age = random.randint(0, 90)
            gender = random.choice(["M", "F", "Other"])
            phone = self.fake.phone_number() if random.random() > 0.1 else ""
            email = self.fake.email() if random.random() > 0.1 else ""
            patients.append([i, name, age, gender, phone, email])
        return patients


class AppointmentGenerator(DataGenerator):
    def __init__(self, num_appointments=2000, num_patients=500):
        super().__init__()
        self.num_appointments = num_appointments
        self.num_patients = num_patients
        self.doctor_names = ["Dr. Patel", "Dr. Chen", "Dr. Smith", "Dr. Lee"]

    def generate(self):
        appointments = []
        for i in range(1, self.num_appointments + 1):
            patient_id = random.randint(1, self.num_patients)
            date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
            date_format = random.choice(["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"])
            date_str = date.strftime(date_format)
            status = random.choice(["attended", "no-show"])
            appointments.append(
                {
                    "appointment_id": i,
                    "patient_id": patient_id,
                    "date": date_str,
                    "doctor": random.choice(self.doctor_names),
                    "status": status,
                }
            )
        return appointments


class ReminderGenerator(DataGenerator):
    def __init__(self, appointments):
        super().__init__()
        self.appointments = appointments

    def _parse_date_flexible(self, s):
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        raise ValueError(f"Unknown date format: {s}")

    def generate(self):
        reminders = []
        for appt in self.appointments:
            reminder_sent = random.random() > 0.2  # 80% get reminders
            reminder_time = None
            if reminder_sent:
                appt_date = self._parse_date_flexible(appt["date"])
                reminder_time = (
                    appt_date - timedelta(days=random.randint(1, 3))
                ).strftime("%Y-%m-%dT%H:%M:%S")
            reminders.append(
                {
                    "appointment_id": appt["appointment_id"],
                    "patient_id": appt["patient_id"],
                    "reminder_sent": reminder_sent,
                    "reminder_time": reminder_time,
                }
            )
        return reminders


# ---------- API Server ----------
class APIServer:
    def __init__(self, appointments):
        self.app = Flask(__name__)
        self.appointments = appointments
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/appointments", methods=["GET"])
        def get_appointments():
            return jsonify(self.appointments)

    def run(self):
        self.app.run(debug=True)


# ---------- Main Runner ----------
if __name__ == "__main__":
    # Patients
    pg = PatientGenerator()
    patients = pg.generate()
    pg.save_to_file(patients, "patients.csv")

    # Appointments
    ag = AppointmentGenerator()
    appointments = ag.generate()
    ag.save_to_file(appointments, "appointments.json")

    # Reminders
    rg = ReminderGenerator(appointments)
    reminders = rg.generate()
    rg.save_to_file(reminders, "reminders.json")

    print("Generated patients.csv, appointments.json, reminders.json")

    # Run API server
    server = APIServer(appointments)
    print("Starting Flask API at http://localhost:5000/appointments")
    server.run()
