import os
import json
import pandas as pd
import requests
import zipfile


# ---------- Base Class ----------
class ETLStep:
    def run(self, *args, **kwargs):
        raise NotImplementedError("Subclasses must implement run()")


# ---------- Extractor ----------
class Extractor(ETLStep):
    def __init__(self, patients_csv, appointments_api, reminders_json):
        self.patients_csv = patients_csv
        self.appointments_api = appointments_api
        self.reminders_json = reminders_json

    def run(self):
        patients_df = pd.read_csv(self.patients_csv)
        appointments_df = pd.DataFrame(
            requests.get(self.appointments_api).json()
        )
        reminders_df = pd.read_json(self.reminders_json)
        return patients_df, appointments_df, reminders_df


# ---------- Transformer ----------
class Transformer(ETLStep):
    @staticmethod
    def _parse_date_flexible(s):
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
            try:
                return pd.to_datetime(s, format=fmt)
            except ValueError:
                continue
        raise ValueError(f"Unknown date format: {s}")

    def run(self, patients_df, appointments_df, reminders_df):
        # Normalize dates
        appointments_df["date"] = appointments_df["date"].apply(
            self._parse_date_flexible
        )

        # Merge
        df = appointments_df.merge(
            reminders_df, on=["appointment_id", "patient_id"], how="left"
        )
        df = df.merge(patients_df, on="patient_id", how="left")

        # Aggregations
        summary = (
            df.groupby(["patient_id", "name"])
            .agg(
                total_appointments=("appointment_id", "count"),
                no_shows=("status", lambda s: (s == "no-show").sum()),
                reminders_sent=("reminder_sent", "sum"),
            )
            .reset_index()
        )
        summary["no_show_rate"] = (
            100 * summary["no_shows"] / summary["total_appointments"]
        )
        return summary


# ---------- Loader ----------
class Loader(ETLStep):
    def __init__(self, output_csv="patient_no_show_summary.csv", output_zip="cleaned_data.zip"):
        self.output_csv = output_csv
        self.output_zip = output_zip

    def run(self, summary_df):
        summary_df.to_csv(self.output_csv, index=False)
        with zipfile.ZipFile(self.output_zip, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(self.output_csv)
        print(f"ETL complete. Clean dataset saved as {self.output_zip}")


# ---------- Pipeline Manager ----------
class Pipeline:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        patients, appointments, reminders = self.extractor.run()
        summary = self.transformer.run(patients, appointments, reminders)
        self.loader.run(summary)


# ---------- Main Runner ----------
if __name__ == "__main__":
    extractor = Extractor(
        patients_csv="patients.csv",
        appointments_api="http://127.0.0.1:5000/appointments",
        reminders_json="reminders.json",
    )
    transformer = Transformer()
    loader = Loader()

    pipeline = Pipeline(extractor, transformer, loader)
    pipeline.run()


