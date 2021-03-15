import os
import json

curr_dir_path = os.path.dirname(os.path.realpath(__file__))
dataset_path = os.path.join(curr_dir_path, "bigquery", "datasets")


class Client:

    def __init__(self):
        pass

    def insert_rows_json(self, table, rows, **kwargs):
        table_arr = table.split('.')
        dataset = table_arr[1]
        table = table_arr[2]
        try:
            file_path = os.path.join(dataset_path, dataset, "{}.json".format(table))
            with open(file_path) as f:
                table_structure = json.loads(f.read())

        except Exception as e:
            raise Exception("404 Table not found - {}.{}".format(dataset, table))

        errors = []

        kwargs = {
            "table_structure": table_structure
        }
        for idx, row in enumerate(rows):
            row_messages = []
            for check in error_checker:
                error_messages = check(row, **kwargs)
                if error_messages:
                    row_messages.append(error_messages)

            if row_messages:
                errors.append({"index": idx, "errors": [row_messages]})

        return errors


def check_table_columns(row, **kwargs):
    messages = []
    err_message = {}
    table_structure = kwargs['table_structure']
    for column in table_structure:
        if table_structure[column] == "REQUIRED":
            if column not in row:
                messages.append("Missing required fields: Msg_0_CLOUD_QUERY_TABLE.{}".format(column.lower()))

    if messages:
        err_message = {"reason": "invalid", "location": "", "debugInfo": "", "message": ','.join(messages)}

    return err_message


error_checker = [
    check_table_columns
]


def main():

    client = Client()
    rows = [{"BLAH": "DATA"}]
    table = 'test.edc_activity.inbound_converter'

    errors = client.insert_rows_json(table, rows)
    print(errors)

    rows = [{"LOGSTASH_ID": "DATA"}]
    table = 'test.edc_activity.inbound_converter'

    errors = client.insert_rows_json(table, rows)
    print(errors)

    rows = [{"BLAH": "DATA"}]
    table = 'test.edc_activity.inbound_converter_ip_lookup'

    try:
        errors = client.insert_rows_json(table, rows)
        print(errors)
    except Exception as e:
        print(str(e))


if __name__ == "__main__":
    main()
