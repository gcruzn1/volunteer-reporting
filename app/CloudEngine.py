"""Class To Handle Cloud Provider
WIP - 
Idea is to abstract usage of cloud provider, easily providing 
a way to swap providers OR even use more than one for resiliency :)
"""
import GoogleClient

class CloudEngine:
    def __init__(self, providerName) -> None:
        self.creds = self.authenticate()
        self.provider = providerName
    
    def authenticate(self):
        self.creds = GoogleClient.CredsServiceAcct()
    
    def get_engine(self):
        return self.creds