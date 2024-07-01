import csv
import json

class Data:
    """
    Represents a data processing class for handling JSON and CSV data.

    Attributes:
        path (str): Path to the data file.
        type (str): Type of data ('json' or 'csv').
        data (list): List of dictionaries representing the data.
        columns (list): List of column names in the data.

    Methods:
        read_json(): Read JSON data from the file.
        read_csv(): Read CSV data from the file.
        read_data(): Read data based on the specified type.
        get_columns(): Get the column names from the data.
        transform_columns(key_mapping): Transform column names using a key mapping.
        join_data(data_A, data_B): Combine data from two Data objects.
        table_transformation(): Transform data into a table format.
        save_data(path): Save data to a CSV file.
    """

    def __init__(self, path, type):
        """
        Initialize a Data object.

        Args:
            path (str): Path to the data file.
            type (str): Type of data ('json' or 'csv').
        """
        self.path = path
        self.type = type
        self.data = self.read_data()
        self.columns = self.get_columns()

    def read_json(self):
        """
        Read JSON data from the file.

        Returns:
            list: List of dictionaries representing the JSON data.
        """
        json_data = []
        with open(self.path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def read_csv(self):
        """
        Read CSV data from the file.

        Returns:
            list: List of dictionaries representing the CSV data.
        """
        csv_data = []
        with open(self.path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    def read_data(self):
        """
        Read data based on the specified type.

        Returns:
            list: List of dictionaries representing the data.
        """
        data = []
        if self.type == 'csv':
            data = self.read_csv()
        elif self.type == 'json':
            data = self.read_json()
        elif self.type == 'list':
            data = self.path
            self.path = 'in memory list'
        return data

    def get_columns(self):
        """
        Get the column names from the data.

        Returns:
            list: List of column names.
        """
        columns = list(self.data[-1].keys())
        return columns

    def transform_columns(self, key_mapping):
        """
        Transform column names using a key mapping.

        Args:
            key_mapping (dict): Mapping of old column names to new column names.
        """
        new_data = []
        for old_dict in self.data:
            temp_dict = {}
            for old_key, value in old_dict.items():
                temp_dict[key_mapping[old_key]] = value
            new_data.append(temp_dict)
        self.data = new_data
        self.columns = self.get_columns()

    @staticmethod
    def join_data(data_A, data_B):
        """
        Combine data from two Data objects.

        Args:
            data_A (Data): First Data object.
            data_B (Data): Second Data object.

        Returns:
            Data: Combined Data object.
        """
        combined_list = []
        combined_list.extend(data_A.data)
        combined_list.extend(data_B.data)
        return Data(combined_list, 'list')

    def table_transformation(self):
        """
        Transform data into a table format.

        Returns:
            list: List of lists representing the data in table format.
        """
        combined_data_table = [self.columns]
        for row in self.data:
            line = []
            for col in self.columns:
                line.append(row.get(col, 'N/A'))
            combined_data_table.append(line)
        return combined_data_table

    def save_data(self, path):
        """
        Save data to a CSV file.

        Args:
            path (str): Path to save the CSV file.
        """
        combined_data_table = self.table_transformation()
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(combined_data_table)
