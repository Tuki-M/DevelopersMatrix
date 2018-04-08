from Helpers.LocalData import LocalData


def lambda_handler(event, context):
    LocalData.CreateAvroFileWithMinData()
    LocalData.ReadFromAvroFile()