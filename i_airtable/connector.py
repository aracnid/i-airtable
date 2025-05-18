"""Airtable connector for Aracnid integration framework.
"""
import os
from typing import Any, Dict, List, Optional

from aracnid_core.base import BaseConnector
from pyairtable import Api, Table


class AirtableConnector(BaseConnector):
    """Airtable connector for Aracnid integration framework.
    """
    def __init__(self, base_id: str, table_name: str):
        """Initialize the Airtable connector.
        Args:
            base_id (str): The Airtable base ID.
            table_name (str): The Airtable table name.
            api_key (str): The Airtable API key.

        Environment Variables:
            AIRTABLE_API_KEY: The Airtable API key.
        """
        # read environment variables
        self.air_api_key = os.environ.get('AIRTABLE_API_KEY')
        if not self.air_api_key:
            raise ValueError('AIRTABLE_API_KEY environment variable is required.')

        # initialize api
        self.api = Api(self.air_api_key)

        # set the base
        self.base_id = base_id
        bases = self.api.bases()
        self.base = None
        for base in bases:
            if base.id == self.base_id:
                self.base = base
                break
        if not self.base:
            raise ValueError(f'Base with ID {self.base_id} not found.')

        # set the table
        self.table_name = table_name
        self.table = Table(self.air_api_key, self.base_id, table_name)
        if not self.table:
            raise ValueError(f'Table with name {self.table_name} not found in base {self.base_id}.')

    def create(self, record: Dict) -> Dict:
        response = requests.post(self.api_url, json={'fields': record}, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def read_one(self, record_id: str) -> Optional[Dict]:
        url = f'{self.api_url}/{record_id}'
        response = requests.get(url, headers=self.headers)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def read_many(self, filters: Optional[Dict] = None) -> List[Dict]:
        records = []
        offset = None
        while True:
            params = {}
            if offset:
                params['offset'] = offset
            if filters:
                formula = ' AND '.join([f'{{{k}}}="{v}"' for k, v in filters.items()])
                params['filterByFormula'] = formula
            response = requests.get(self.api_url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            records.extend(data.get('records', []))
            offset = data.get('offset')
            if not offset:
                break
        return records

    def update(self, record_id: str, changes: Dict) -> Dict:
        url = f'{self.api_url}/{record_id}'
        response = requests.patch(url, json={'fields': changes}, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def replace(self, record_id: str, new_record: Dict) -> Dict:
        url = f'{self.api_url}/{record_id}'
        response = requests.put(url, json={'fields': new_record}, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def delete_one(self, record_id: str, hard: bool = False) -> bool:
        if not hard:
            return bool(self.update(record_id, {'is_deleted': True}))
        url = f'{self.api_url}/{record_id}'
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 200:
            return True
        response.raise_for_status()
        return False

    def delete_many(self, filters: Optional[Dict] = None, hard: bool = False) -> int:
        records = self.read_many(filters)
        count = 0
        for record in records:
            record_id = record.get('id')
            if record_id and self.delete_one(record_id, hard=hard):
                count += 1
        return count

    def get_source_name(self) -> str:
        if not self.base:
            raise ValueError(f'Base with ID {self.base_id} not found.')

        return f'airtable.{self.base.name}.{self.table_name}'
