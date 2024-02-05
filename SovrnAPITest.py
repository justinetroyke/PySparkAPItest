import requests

api_endpoint = 'https://api.tomorrow.io/v4/weather/forecast?location=42.3478,-71.0466&apikey=fnzkTmVUWX0TyKAIyuVwwUINQvLCinPU'

try:
    # GET request to API endpoint
    response = requests.get(api_endpoint)

    # Check success status
    if response.status_code == 200:
        # Parse the JSON data from the response
        data = response.json()
    else:
        print(response.status_code)
finally: 
    print('Done') 


