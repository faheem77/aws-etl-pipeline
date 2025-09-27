
import pandas as pd
import re
import json
def column_rename(df):
    rename_map = {
        "propertyStatus": "property_status",
        "numberOfBeds": "bedrooms",
        "numberOfBaths": "bathrooms",
        "sqft": "square_feet",
        "addr1": "address_line_1",
        "addr2": "address_line_2",
        "streetNumber": "street_number",
        "streetName": "street_name",
        "streetType": "street_type",
        "preDirection": "pre_direction",
        "unitType": "unit_type",
        "unitNumber": "unit_number",
        "zipcode": "zip_code",
        "propertyType": "property_type",
        "yearBuilt": "year_built",
        "presentedBy": "presented_by",
        "brokeredBy": "brokered_by",
        "realtorMobile": "presented_by_mobile",
        "sourcePropertyId": "mls",
        "openHouse": "open_house",
        "compassPropertyId": "compass_property_id",
        "pageLink": "page_link"
    }

    # Apply rename
    df = df.rename(columns=rename_map)
    return df
def change_status(df):
    STATUS_MAPPING = {
    "Active Under Contract": "Pending",
    "New": "Active",
    "Closed": "Sold"}
    df['property_status'] = df['property_status'].replace(STATUS_MAPPING)
    return df
def parse_name(df):
        # Split into parts (max 3 to handle first, middle, last)
    name_parts = df["presented_by"].str.split(n=2, expand=True)

    # Assign new columns
    df["presented_by_first_name"] = name_parts[0]
    df["presented_by_middle_name"] = name_parts[1]
    df["presented_by_last_name"] = name_parts[2]
    df = df.drop(columns=["presented_by"])
    return df
def transform_open_house(df: pd.DataFrame) -> pd.DataFrame:
    """Parse open_house JSON column and extract oh_startTime, oh_company, oh_contactName."""

    def parse_open_house(x):
        if pd.isna(x):
            return {}
        if isinstance(x, str):
            try:
                x = json.loads(x)
            except json.JSONDecodeError:
                return {}
        # Handle list case: take the first element
        if isinstance(x, list) and len(x) > 0:
            return x[0]  # pick the first open house if multiple exist
        if isinstance(x, dict):
            return x
        return {}

    parsed = df["open_house"].apply(parse_open_house)

    # Extract keys into new columns safely
    df["oh_startTime"] = parsed.apply(lambda d: d.get("oh_startTime") if isinstance(d, dict) else None)
    df["oh_company"] = parsed.apply(lambda d: d.get("oh_company") if isinstance(d, dict) else None)
    df["oh_contactName"] = parsed.apply(lambda d: d.get("oh_contactName") if isinstance(d, dict) else None)

    # Drop original column
    df = df.drop(columns=["open_house"])
    return df

def generate_full_address(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a full_address column from address parts with proper formatting."""
    
    # Replace NaNs with empty strings
    df = df.fillna({"address_line_1": "", "address_line_2": "", "city": "", "state": "", "zip_code": ""})
    
    def build_address(row):
        parts = [
            row["address_line_1"].strip(),
            row["address_line_2"].strip(),
            row["city"].strip(),
            row["state"].strip(),
            str(row["zip_code"]).strip(),
        ]
        # Filter out empty strings and join with comma
        return ", ".join([p for p in parts if p])

    df["full_address"] = df.apply(build_address, axis=1)
    return df
def split_emails(df: pd.DataFrame) -> pd.DataFrame:
    """Split email column into email_1 and email_2."""
    def process_email(x):
        if pd.isna(x):
            return [None, None]
        # Normalize separators to comma
        parts = str(x).replace(";", ",").replace(" ", ",").split(",")
        parts = [p.strip() for p in parts if p.strip()]  # clean up spaces/empties
        # Take first 2 emails only
        email_1 = parts[0] if len(parts) > 0 else None
        email_2 = parts[1] if len(parts) > 1 else None
        return [email_1, email_2]

    df[["email_1", "email_2"]] = df["email"].apply(process_email).apply(pd.Series)
    return df
def generate_transaction_id(df: pd.DataFrame) -> pd.DataFrame:
    """Generate transaction ID by slugifying MLS + address + city + state + zip."""

    def slugify(value: str) -> str:
        value = str(value).lower()
        value = re.sub(r'[^a-z0-9]+', '-', value)   # replace non-alphanumeric with hyphen
        value = re.sub(r'-+', '-', value).strip('-') # collapse multiple hyphens
        return value

    # Fill NaN with empty strings to avoid errors
    df = df.fillna({
        "mls": "", "address_line_1": "", "address_line_2": "",
        "city": "", "state": "", "zip_code": ""
    })

    df["id"] = df.apply(
        lambda row: slugify(
            f"{row['mls']} {row['address_line_1']} {row['address_line_2']} "
            f"{row['city']} {row['state']} {row['zip_code']}"
        ),
        axis=1
    )
    return df

def clean_phone_numbers(df: pd.DataFrame, column: str = "presented_by_mobile") -> pd.DataFrame:
    """Clean phone numbers: keep only digits and limit to 10 digits."""
    
    def clean_number(x):
        if pd.isna(x):
            return None
        # Keep digits only
        digits = re.sub(r"\D", "", str(x))
        # Limit to last 10 digits (to preserve correct number if country code exists)
        return digits[-10:] if digits else None

    df[column] = df[column].apply(clean_number)
    return df

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop columns which:
    1. Are unnamed (start with 'Unnamed')
    2. Have all values as NaN
    """
    # Filter columns that start with 'Unnamed'
    unnamed_cols = [col for col in df.columns if col.startswith("Unnamed")]
    
    # Drop unnamed columns if all values are NaN
    for col in unnamed_cols:
        if df[col].isna().all():
            df.drop(columns=[col], inplace=True)
    for col in ["price", "bedrooms", "bathrooms", "zip_code", "latitude", "longitude"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df