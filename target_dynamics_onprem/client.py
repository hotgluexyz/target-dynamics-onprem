"""WoocommerceSink target sink class, which handles writing streams."""

from target_hotglue.client import HotglueSink
from requests_ntlm import HttpNtlmAuth
import backoff
import requests
import json
from singer_sdk.exceptions import RetriableAPIError
from target_hotglue.common import HGJSONEncoder
from datetime import datetime
import ast


class DynamicOnpremSink(HotglueSink):

    def __init__(
        self,
        target,
        stream_name,
        schema,
        key_properties,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

    @property
    def company_key(self):
        base_url = f"{self.config.get('url_base')}"
        if "api" in base_url:
            company_key = "companies"
        elif "OData" in base_url:
            company_key = "Company"
        return company_key

    @property
    def base_url(self):
        base_url = f"{self.config.get('url_base')}{self.company_key}"
        self.logger.info(f"BASE URL: {base_url}")
        return base_url

    @property
    def http_headers(self):
        return {}
    
    params = {"$format": "json"}
    
    def clean_convert(self, input):
        if isinstance(input, list):
            return [self.clean_convert(i) for i in input]
        elif isinstance(input, dict):
            output = {}
            for k, v in input.items():
                v = self.clean_convert(v)
                if isinstance(v, list):
                    output[k] = [i for i in v if (i)]
                elif v:
                    output[k] = v
            return output
        elif isinstance(input, datetime):
            return input.isoformat()
        elif input:
            return input
    
    def convert_date(self, date):
        converted_date = date.split("T")[0]
        return converted_date
    
    def request_api(self, http_method, endpoint=None, params={}, request_data=None, headers={}):
        """Request records from REST endpoint(s), returning response records."""
        resp = self._request(http_method, endpoint, params=params, headers=headers, request_data=request_data)
        return resp
    
    def get_endpoint(self, record):
        #use subsidiary as company if passed, else use company from config
        company_id = record.get("subsidiary") or self.config.get("company_id")
        if self.company_key == "Company":
            return f"('{company_id}')" + self.endpoint
        elif self.company_key == "companies":
            return f"({company_id})" + self.endpoint
    
    def check_bill_amount(self, bill_keys, total_amount, total_field):
        id = bill_keys
        if isinstance(bill_keys, list):
            id = ""
            for key in bill_keys:
                id = id + f"'{key}'"
        #get bill
        endpoint = f"{self.endpoint}({id})"
        purchase_order_lines = self.request_api(
            "POST",
            endpoint=endpoint,
            params=self.params,
        )
        if purchase_order_lines.json().get(total_field, 0) != total_amount:
            purchase_order_lines = self.request_api(
                "DELETE",
                endpoint=endpoint,
                params=self.params,
            )

    
    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, auth=None, params={}, request_data=None, headers={}
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data
            else None
        )

        if self.config.get("basic_auth") == True:
            auth = (self.config.get("username"), self.config.get("password"))
        else:
            auth = HttpNtlmAuth(self.config.get("username"), self.config.get("password"))        

        self.logger.info(f"MAKING {http_method} REQUEST")
        self.logger.info(f"URL {url} params {params} data {data}")
        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            auth=auth
        )
        self.logger.info("response!!")
        self.logger.info(response.status_code)
        self.logger.info(f"RESPONSE TEXT {response.text} STATUS CODE {response.status_code}")
        self.validate_response(response)
        return response

    def parse_objs(self, obj):
        try:
            try:
                return ast.literal_eval(obj)
            except:
                return json.loads(obj)
        except:
            return obj
    