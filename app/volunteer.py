"""example file, not used for prod"""
import pandas as pd


class Volunteer:

    def __init__(
            self, 
            id: int, 
            full_name: str, 
            phone_number: str,
            can_contact: bool,
            delegation_rules: dict,
            is_active: bool = True,
            metrics: dict[str: dict] = {}
        ):

        self.id = id
        self.full_name = full_name
        self.phone_number = phone_number
        self.can_contact = can_contact
        self.delegation_rules = delegation_rules
        self.is_active = is_active

        self.metrics = metrics

    def __str__(self) -> str:
        return f"{self.id}: {self.full_name}"

    def add_metrics(self, report_data: dict[str: list]):
        """Add metrics per month & year. Will overwrite data if exists"""
        self.metrics.update(report_data)
    
    def remove_metrics(self, report_year_month: str):
        """Remove report month. Raises KeyError if it does not exist"""
        result = self.metrics.pop(report_year_month)
        return f"Removed {result}"
    
    def get_report_summary(self):
        return {
            self.id: {self.full_name: self.metrics}
        }
    
    def get_report_by_period(self, reporting_period: str):
        return {
            self.id: {
                self.full_name: self.metrics[reporting_period]
            }
        }

