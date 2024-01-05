"""Dynamics-onprem target sink class, which handles writing streams."""
import json
from target_dynamics_onprem.client import DynamicOnpremSink
from datetime import datetime


class Vendors(DynamicOnpremSink):
    """Dynamics-onprem target sink class."""

    endpoint = "/workflowVendors"
    available_names = ["Vendors"]
    name = "Vendors"

    def preprocess_record(self, record: dict, context: dict) -> None:
        self.endpoint = self.get_endpoint(record)
        phoneNumbers = record.get("phoneNumber")
        address = record.get("addresses")
        mapping = {
            "name": record.get("vendorName"),
            "name2": record.get("contactName"),
            "eMail": record.get("emailAddress"),
            "phoneNumber": phoneNumbers[0] if phoneNumbers else None,
            "currencyCode": record.get("currency"),
        }

        if address:
            address = address[0]
            mapping["address"] = address.get("line1")
            mapping["address2"] = address.get("line2")
            mapping["city"] = address.get("city")
            mapping["county"] = address.get("state")
            mapping["countryRegionCode"] = address.get("country")
            mapping["postCode"] = address.get("postalCode")

        mapping = self.clean_convert(mapping)
        return mapping

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            vendor = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record, params=self.params
            )
            vendor_id = vendor.json()["No"]
            self.logger.info(f"BuyOrder created succesfully with Id {vendor_id}")
            return vendor_id, True, state_updates
        
class Items(DynamicOnpremSink):
    """Dynamics-onprem target sink class."""

    endpoint = "/workflowItems"
    available_names = ["Items"]
    name = "Items"

    def preprocess_record(self, record: dict, context: dict) -> None:
        self.endpoint = self.get_endpoint(record)
        mapping = {
            "description": record.get("name"),
            "type": record.get("type"),
            "reorderPoint": record.get("reorderPoint"),
            "taxGroupCode": record.get("taxCode"),
            "itemCategoryCode": record.get("category"),
        }
        if record.get("billItem", record.get("invoiceItem")):
            bill_item = record.get("billItem",record.get("invoiceItem"))
            bill_item = json.loads(bill_item)
            mapping["description2"] = bill_item.get("description")
            mapping["unitPrice"] = bill_item.get("unitPrice")

        mapping = self.clean_convert(mapping)
        return mapping

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            item = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record, params=self.params
            )
            item_id = item.json()["No"]
            self.logger.info(f"Item created succesfully with Id {item_id}")
            return item_id, True, state_updates
        
class PurchaseDocuments(DynamicOnpremSink):
    """Dynamics-onprem target sink class."""

    endpoint = "/purchaseDocuments"
    @property
    def name(self):
        return self.stream_name
    available_names = ["PurchaseOrders", "Bills"]
    bills_default = True

    def preprocess_record(self, record: dict, context: dict) -> None:
        self.endpoint = self.get_endpoint(record)
        dueDate = None
        if record.get("dueDate"):
            dueDate = self.convert_date(record.get("dueDate"))
        documentType = "Order" if self.stream_name == "PurchaseOrders" else "Invoice"
        purchase_order_map = {
            "buyFromVendorNumber": record.get("vendorId"),
            "payToVendorNumber": record.get("vendorId"),
            "payToName": record.get("vendorName"),
            "currencyCode": record.get("currency"),
            "dueDate": dueDate,
            "locationCode": record.get("locationId"),
            "documentType": documentType,
            "balAccountType": record.get("accountName"),
        }
        po_custom_fields = record.get("customFields")
        if po_custom_fields:
            [purchase_order_map.update({cf.get("name"): cf.get("value")}) for cf in po_custom_fields]
        lines = []
        # add correlative line number
        line_number = 0
        for line in record.get("lineItems", []):
            serviceDate = None
            if line.get("serviceDate"):
                serviceDate = self.convert_date(line.get("serviceDate"))
            line_map = {
                "quantity": line.get("quantity"),
                "jobUnitPrice": line.get("unitPrice"),
                "jobLineDiscountAmount": line.get("discount"),
                "taxGroupCode": line.get("taxCode"),
                "description": line.get("productName"),
                "number": line.get("productId") if documentType == "Order" else line.get("accountNumber"),
                "orderDate": serviceDate,
                "type": "Item" if documentType == "Order" else "G/L Account",
                "directUnitCost": line.get("unitPrice"),
                "lineNumber": line_number
            }
            line_number = line_number + 1
            #map custom fields
            custom_fields = line.get("customFields")
            if custom_fields:
                [line_map.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]
            lines.append(line_map)

        payload = {
            "purchase_order" : purchase_order_map,
            "lines": lines
        }
        mapping = self.clean_convert(payload)
        return mapping

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            purchase_order = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record.get("purchase_order"), params=self.params
            )
            purchase_order = purchase_order.json()
            if purchase_order and purchase_order.get("number"):
                pol_endpoint = self.endpoint.split("/")[0] + "/purchaseDocumentLines"
                
                for line in record.get("lines", []):
                    line["documentType"] = purchase_order.get("documentType")
                    line["documentNumber"] = purchase_order.get("number")
                    try:
                        purchase_order_lines = self.request_api(
                            "POST", endpoint=pol_endpoint, request_data=line
                        )
                    except Exception as e:
                        self.logger.info(f"Posting line {line} has failed")
                        self.logger.info("Deleting purchase order header")
                        delete_endpoint = f"{self.endpoint}({purchase_order.get('id')})"
                        purchase_order_lines = self.request_api(
                            "DELETE", endpoint=delete_endpoint
                        )
                        raise Exception(e)

            purchase_order_id = purchase_order["number"]
            self.logger.info(f"purchase_order created succesfully with Id {purchase_order_id}")
            return purchase_order_id, True, state_updates

class PurchaseInvoice(DynamicOnpremSink):
    """Dynamics-onprem target sink class."""

    endpoint = "/Purchase_Invoice"
    @property
    def name(self):
        return self.stream_name
    available_names = ["PurchaseInvoices", "Bills"]
    bills_default = False

    def preprocess_record(self, record: dict, context: dict) -> None:
        self.logger.info("PROCCESSING RECORD")
        self.endpoint = self.get_endpoint(record)
        dueDate = None
        if record.get("dueDate"):
            dueDate = self.convert_date(record.get("dueDate"))
        
        issueDate = None
        if record.get("issueDate"):
            issueDate = self.convert_date(record.get("issueDate"))

        purchase_order_map = {
            "Buy_from_Vendor_Name": record.get("vendorName"),
            "Buy_from_Vendor_No": record.get("vendorId"),
            "Due_Date": dueDate,
            "Invoice_Receipt_Date": issueDate,
            "Document_Type": "Invoice"
        }
        # map purchase order custom fields
        po_custom_fields = record.get("customFields")
        if po_custom_fields:
            [purchase_order_map.update({cf.get("name"): cf.get("value")}) for cf in po_custom_fields]
        
        # map lines
        lines = []
        pi_lines = record.get("lineItems")
        if isinstance(pi_lines, str):
            pi_lines = self.parse_objs(pi_lines)
        for line in pi_lines:
            type = "G/L Account" if line.get("accountNumber") else "Item" if line.get("productNumber") else None
            line_map = {
                "Line_Amount": line.get("totalPrice"),
                "Description": line.get("description"),
                "Type": type,
                "No": str(line.get("accountNumber")),
                "Quantity": line.get("quantity", 1),
                "Direct_Unit_Cost": line.get("unitPrice", line.get("totalPrice")),
            }

            custom_fields = line.get("customFields")
            if custom_fields:
                [line_map.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]

            lines.append(line_map)

        payload = {
            "purchase_invoice" : purchase_order_map,
            "lines": lines
        }
        mapping = self.clean_convert(payload)

        return mapping

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            purchase_order = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record.get("purchase_invoice"), params=self.params
            )
            purchase_order = purchase_order.json()
            purchase_order_no = purchase_order.get("No")
            if purchase_order and purchase_order_no:
                pol_endpoint = self.endpoint.split("/")[0] + "/Purchase_InvoicePurchLines"
                self.logger.info("Posting purchase invoice lines")
                for line in record.get("lines"):
                    line["Document_Type"] = "Invoice"
                    line["Document_No"] = purchase_order_no
                    try:
                        purchase_order_lines = self.request_api(
                            "POST", endpoint=pol_endpoint, request_data=line, params=self.params
                        )
                    except Exception as e:
                        self.logger.info(f"Posting line {line} has failed")
                        self.logger.info("Deleting purchase order header")
                        delete_endpoint = f"{self.endpoint}({purchase_order_no})"
                        purchase_order_lines = self.request_api(
                            "DELETE", endpoint=delete_endpoint
                        )
                        raise Exception(e)

            self.logger.info(f"purchase_invoice created succesfully with No {purchase_order_no}")
            return purchase_order_no, True, state_updates


# class PurchaseInvoice(DynamicOnpremSink):
    """Dynamics-onprem target sink class."""

    endpoint = "/PostedPurchaseInvoice"
    @property
    def name(self):
        return self.stream_name
    available_names = ["PurchaseInvoices", "Bills"]
    bills_default = False

    def preprocess_record(self, record: dict, context: dict) -> None:
        self.logger.info(f"PROCESSING RECORD")
        self.logger.info(f"ENDPOINT {self.endpoint}")
        self.endpoint = self.get_endpoint(record)
        dueDate = None
        if record.get("dueDate"):
            dueDate = self.convert_date(record.get("dueDate"))
        
        issueDate = None
        if record.get("issueDate"):
            issueDate = self.convert_date(record.get("issueDate"))

        purchase_order_map = {
            "Buy_from_Vendor_Name": record.get("vendorName"),
            "Buy_from_Vendor_No": record.get("vendorId"),
            "Due_Date": dueDate,
            "Document_Date": issueDate,
        }
        # map purchase order custom fields
        po_custom_fields = record.get("customFields")
        if po_custom_fields:
            [purchase_order_map.update({cf.get("name"): cf.get("value")}) for cf in po_custom_fields]
        
        # map lines
        lines = []
        pi_lines = record.get("lineItems")
        if isinstance(pi_lines, str):
            pi_lines = self.parse_objs(pi_lines)
        for line in pi_lines:
            type = "G/L Account" if line.get("accountNumber") else "Item" if line.get("productNumber") else None
            line_map = {
                "Line_Amount": line.get("totalPrice"),
                "Description": line.get("description"),
                "Type": type,
                "No": str(line.get("accountNumber")),
                "Quantity": line.get("quantity", 1),
                "Direct_Unit_Cost": line.get("unitPrice", line.get("totalPrice")),
            }

            custom_fields = line.get("customFields")
            if custom_fields:
                [line_map.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]

            lines.append(line_map)

        payload = {
            "purchase_invoice" : purchase_order_map,
            "lines": lines
        }
        mapping = self.clean_convert(payload)

        return mapping

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            self.logger.info(f"USING ENDPOINT {self.endpoint} FOR HEADERS")
            purchase_order = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record.get("purchase_invoice"), params=self.params
            )
            purchase_order = purchase_order.json()
            purchase_order_no = purchase_order.get("No")
            if purchase_order and purchase_order_no:
                pol_endpoint = self.endpoint.split("/")[0] + "/PostedPurchaseInvoicePurchInvLines"
                self.logger.info(f"USING ENDPOINT {pol_endpoint} FOR HEADERS")
                self.logger.info("Posting purchase invoice lines")
                for line in record.get("lines"):
                    line["Document_No"] = purchase_order_no
                    try:
                        purchase_order_lines = self.request_api(
                            "POST", endpoint=pol_endpoint, request_data=line, params=self.params
                        )
                    except Exception as e:
                        self.logger.info(f"Posting line {line} has failed")
                        self.logger.info("Deleting purchase order header")
                        delete_endpoint = f"{self.endpoint}({purchase_order_no})"
                        purchase_order_lines = self.request_api(
                            "DELETE", endpoint=delete_endpoint
                        )
                        raise Exception(e)

            self.logger.info(f"purchase_invoice created succesfully with No {purchase_order_no}")
            return purchase_order_no, True, state_updates