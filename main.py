import os
import re
import json
import time
import pandas as pd
import requests
import datetime

input_path = "./input_data/MOCK_DATA.csv"
output_path = "./output_data/OUTPUT.csv"
base_url = "https://api.close.com/api/v1"
api_key = os.environ["close_project_api_key"]
start_date = "1950-01-01"
end_date = "2022-12-31"

state_mapping = {
    "California": "CA",
    "Delaware": "DE",
    "Minnesota": "MN",
    "New Mexico": "NM",
    "New York": "NY",
    "CA": "California",
    "DE": "Delaware",
    "MN": "Minnesota",
    "NM": "New Mexico",
    "NY": "New York",
}

def segment_leads_by_state(leads: list) -> None:
    """ segment search results, write to csv """

    parsed_leads = []
    for lead in leads:
        parsed_lead = {
            "lead_name": lead["display_name"],
            "state": translate_state_names(lead["addresses"][0]["state"]),
            "revenue": lead.get(
                "custom.cf_ZyBTifqGw97jHJwjk2q7MXdFolbmvkWdxUkNhgyLF0T", 0
            ),
        }
        parsed_leads.append(parsed_lead)
    leads_df = pd.DataFrame(data=parsed_leads)
    (leads_df["revenue"]).replace("[\$,]", "", regex=True, inplace=True)
    leads_df["revenue"] = (leads_df["revenue"]).astype(float)
    leads_df.sort_values("revenue", ascending=False, inplace=True)
    segment_df = pd.DataFrame()
    segment_df["US State"] = leads_df.groupby("state")["state"].first()
    segment_df["Total Number of Leads"] = leads_df.groupby("state")["lead_name"].count()
    segment_df["The Lead With Most Revenue"] = (leads_df.groupby("state").first())["lead_name"]
    segment_df["Total Revenue"] = leads_df.groupby("state")["revenue"].sum()
    segment_df["Median Revenue"] = leads_df.groupby("state")["revenue"].median()
    segment_df.to_csv(output_path, index=False)

def get_lead(lead_id: str) -> dict:
    """ get indivdual close lead """

    get_lead_endpoint = f"{base_url}/lead/{lead_id}/"
    response = requests.get(url=get_lead_endpoint, auth=(api_key, ""))
    return json.loads(response.content)

def get_leads_by_date_range(start_date: str, end_date: str) -> list:
    """ use search endpoint to interrogate leads """

    query_results = []
    end_of_results = False
    search_endpoint = f"{base_url}/data/search/"
    query = {
        "limit": None,
        "query": {
            "negate": False,
            "queries": [
                {"negate": False, "object_type": "lead", "type": "object_type"},
                {
                    "negate": False,
                    "queries": [
                        {
                            "negate": False,
                            "queries": [
                                {
                                    "condition": {
                                        "before": {
                                            "type": "fixed_local_date",
                                            "value": end_date,
                                            "which": "end",
                                        },
                                        "on_or_after": {
                                            "type": "fixed_local_date",
                                            "value": start_date,
                                            "which": "start",
                                        },
                                        "type": "moment_range",
                                    },
                                    "field": {
                                        "custom_field_id": "cf_8IDTqfjWod1se2FKcEdKXyB5k67ApyTYBMO27Nz0vbk",
                                        "type": "custom_field",
                                    },
                                    "negate": False,
                                    "type": "field_condition",
                                }
                            ],
                            "type": "and",
                        }
                    ],
                    "type": "and",
                },
            ],
            "type": "and",
        },
        "results_limit": None,
        "include_counts": True,
        "_limit": 25,
        "sort": []
    }
    while end_of_results == False:
        print("searching for matching leads")
        response = requests.post(url=search_endpoint, json=query, auth=(api_key, ""))
        if response.status_code == 200:
            content = json.loads(response.content)
            query_results.extend(content["data"])
            if content["cursor"]:
                query["cursor"] = content["cursor"]
            else:
                end_of_results = True
        time.sleep(.5)    
    return query_results

def create_lead(lead: dict) -> bool:
    """ create a lead in close """

    create_lead_endpoint = f"{base_url}/lead/"
    response = requests.post(url=create_lead_endpoint, json=lead, auth=(api_key, ""))
    if response.status_code == 200:
        print("successfully created lead")
        return True
    else:
        return False

def translate_state_names(state: str) -> str:
    """translate full state name to abbreviation"""

    if not state:
        return ""
    if state_mapping[state]:
        return state_mapping[state]
    
def is_valid_email(email: str) -> bool:
    """ check string against accepted email pattern """

    if re.match(r"^[a-z0-9]+@[a-z]+\.[a-z]+$", email.lower()):
        return True
    return False

def clean_email(email: str) -> list:
    """ simple implementation to process strings for emails"""

    cleaned_emails = []
    split_chars = ["\n", ";", ","]

    if email:
        if any(map(email.__contains__, split_chars)):
            for split_char in split_chars:
                if split_char in email:
                    emails = email.split(split_char)
                    for email in emails:
                        if email and is_valid_email(email):
                            cleaned_emails.append({"type": "office", "email": email})
        else:
            if is_valid_email(email):
                cleaned_emails.append({"type": "office", "email": email})
    return cleaned_emails

def clean_phone_number(phone: str) -> str:
    """compose a valid string of digits from input """

    clean_phone = []
    if phone:
        for char in phone:
            if char.isdigit():
                clean_phone.append(char)
    if clean_phone and len(clean_phone) >= 10 and len(clean_phone) <= 13:
        return "".join(clean_phone)
    else:
        return ""

def format_founding_date(founding_date_string: str) -> str | None:
    """translate incoming format to iso 8601 date """

    if founding_date_string:
        founding_date = datetime.datetime.strptime(founding_date_string, "%d.%m.%Y")
        return founding_date.strftime("%Y-%m-%d")
    else:
        return None

def transform_leads(fp: str) -> list[dict]:
    """ read in a csv and translate to close schema """

    leads = []
    leads_df = (pd.read_csv(fp)).sort_values(by=["Company"])
    for company in (leads_df["Company"]).unique():
        lead = {"name": company, "contacts": [], "addresses": [{}]}
        contacts_df = leads_df[leads_df["Company"] == company]
        contacts_df = contacts_df.fillna("")
        lead[
            "custom.cf_8IDTqfjWod1se2FKcEdKXyB5k67ApyTYBMO27Nz0vbk"
        ] = format_founding_date((contacts_df.iloc[0])["custom.Company Founded"])
        revenue = (contacts_df.iloc[0])["custom.Company Revenue"]
        lead["custom.cf_ZyBTifqGw97jHJwjk2q7MXdFolbmvkWdxUkNhgyLF0T"] = (
            revenue if revenue else None
        )
        lead["addresses"][0]["state"] = translate_state_names(
            (contacts_df.iloc[0])["Company US State"]
        )
        contacts_df = contacts_df[["Contact Name", "Contact Emails", "Contact Phones"]]
        contacts_df["Contact Name"] = (contacts_df["Contact Name"]).str.upper()
        contacts_df["Contact Phones"] = (contacts_df["Contact Phones"]).map(
            lambda p: clean_phone_number(p)
        )
        contacts_df = contacts_df.rename(
            columns={
                "Contact Name": "name",
                "Contact Emails": "emails",
                "Contact Phones": "phones",
            }
        )
        contacts = contacts_df.to_dict(orient="records")
        for contact in contacts:
            contact["phones"] = [{"type": "office", "phone": contact["phones"]}]
            contact["emails"] = clean_email(contact["emails"])
        lead["contacts"] = contacts
        leads.append(lead)
    return leads

def import_leads():
    leads = transform_leads(input_path)
    for lead in leads:
        create_lead(lead)
        time.sleep(0.5)

def segment_leads():
    query_results = get_leads_by_date_range(start_date, end_date)
    leads = []
    for result in query_results:
        lead = get_lead(result["id"])
        leads.append(lead)
        time.sleep(0.5)
    segment_leads_by_state(leads)


import_leads()
segment_leads()