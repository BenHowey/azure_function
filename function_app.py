import datetime
import json
import logging
import random
from io import BytesIO

import azure.functions as func
import pandas as pd
from requests_toolbelt.multipart import decoder

app = func.FunctionApp()


def check_col_headings(df):
    expected_cols = ["lineItemID", "previousLineItemID",
                     "employeeID/employeePpsn",	"employeeID/employmentID",
                     "employerReference", "name/firstName",	"name/familyName",
                     "address/addressLines/0/addressLine", "address/addressLines/1/addressLine",
                     "address/addressLines/2/addressLine", "address/county", "address/eircode",
                     "address/countryCode",	"dateOfBirth", "category", "subCategory", "numberOfDays",
                     "paymentDate",	"amount"]

    if df.columns.to_list() == expected_cols:
        return True
    else:
        return False


def nest_data(row):
    """Function to split out column headings into json nesting

    Args:
        row (pandas dataseries): a row of a dataframe

    Returns:
        dict: Dictionary nested in accordance with ERR document
    """
    result = {}
    for col, val in row.items():
        # if pd.notna(val):  # Only include non-null values
        if val != 'null':  # Only include non-null values
            keys = col.split('/')
            d = result
            for key in keys[:-1]:
                if key not in d:
                    d[key] = {}
                d = d[key]
            d[keys[-1]] = val

    # Special handling for addressLines to ensure it's a list of dicts
    if "address" in result and "addressLines" in result["address"]:
        address_lines = result["address"]["addressLines"]
        # Convert dict to list of dicts
        result["address"]["addressLines"] = [{"addressLine": v['addressLine']} for k, v in address_lines.items() if v is not None]

    return result


def clean_data(data):
    # Convert the datetime columns to datetime if they are not already
    logging.info('In cleaning function')
    data['dateOfBirth'] = pd.to_datetime(data['dateOfBirth'])
    data['paymentDate'] = pd.to_datetime(data['paymentDate'])

    # fill the lineItemID with sequential no
    data['lineItemID'] = data.index.to_series().apply(lambda x: f'Line_{x}')
    # make sure the dates have the correct format
    data['dateOfBirth'] = data['dateOfBirth'].dt.strftime('%Y-%m-%d')
    data['paymentDate'] = data['paymentDate'].dt.strftime('%Y-%m-%d')

    # uppercase and _ the category and subcategory
    if data['category'].notna().any():
        data['category'] = data['category'].str.upper().str.replace(' ', '_')

    if data['subCategory'].notna().any():
        data['subCategory'] = data['subCategory'].str.upper().str.replace(' ', '_')

    logging.info('Midway')

    # strip away any trailing spaces
    if data['employeeID/employeePpsn'].notna().any():
        data['employeeID/employeePpsn'] = data['employeeID/employeePpsn'].str.strip()

    # convert the empyment ID to a str
    if data['employeeID/employmentID'].notna().any():
        data['employeeID/employmentID'] = data['employeeID/employmentID'].astype(str)

    # fill na with null str
    data.fillna('null', inplace=True)

    data['amount'] = data['amount'].round(2)
    logging.info('returning data from func')
    return data


@app.function_name(name="HttpTrigger1")
@app.route(route="hello", auth_level=func.AuthLevel.FUNCTION)
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return func.HttpResponse(
        "This HTTP triggered function executed successfully.",
        status_code=200
        )


@app.function_name(name="HttpTrigger2")
@app.route(route="hello2", auth_level=func.AuthLevel.ANONYMOUS)
def test_function2(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    print('This is a print statement')
    print(req.get_body())
    return func.HttpResponse(
        "This HTTP triggered function executed successfully 22222.",
        status_code=200
        )


@app.function_name(name="postFunc")
@app.route(route="file", auth_level=func.AuthLevel.ANONYMOUS)
def postFunc(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    content = req.get_body()
    content_type = req.headers.get('Content-Type')
    # Decode the multipart form-data
    multipart_data = decoder.MultipartDecoder(content, content_type)
    for part in multipart_data.parts:
        csv_file = part.content
        logging.info('Reading in data')
        data = pd.read_csv(BytesIO(csv_file))
        logging.info('Cleaning data')
        data = clean_data(data)
        logging.info('Nesting data')
        nested_json = data.apply(nest_data, axis=1).to_list()

        rand_submission_id = random.randint(0, 1000000)

        # add this to the root template
        output_json_data = {
            "requestType": "EnhancedReportingSubmission",
            "employerRegistrationNumber": "0071860E",
            "taxYear": datetime.datetime.now().year,
            "softwareUsed": "RNLI_csv2json",
            "softwareVersion": "1.0",
            "enhancedReportingRunReference": f"RNLI_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "submissionID": f"Sub_{rand_submission_id}",
            "requestBody": {
                "expensesBenefits": nested_json,
                "lineItemIDsToDelete": []
            }
        }

        # nested_json = data.apply(nest_data, axis=1).to_list()
        logging.info(f'Converting to json {type(nested_json)}')
        # summary = {"expensesBenefits": nested_json}
        summary = json.dumps(output_json_data, indent=4)
        # logging.info('Returning the response')
    return func.HttpResponse(
        summary,
        status_code=200
        )
