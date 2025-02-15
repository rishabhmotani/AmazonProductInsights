import requests
import pandas as pd
from bs4 import BeautifulSoup
import boto3
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor  # Import for parallel processing
from boto3.dynamodb.conditions import Attr


def normalize_response(product_details):
    """
    Standardize the response format for both cached and dynamically fetched data.
    Includes robust error handling and default values for all fields.

    Args:
        product_details (list): List of product dictionaries to normalize

    Returns:
        list: List of standardized product dictionaries
    """
    if not isinstance(product_details, list):
        print(f"Error: Expected list but got {type(product_details)}")
        return []

    standardized_products = []

    for index, product in enumerate(product_details):
        try:
            if not isinstance(product, dict):
                print(f"Error: Product at index {index} is not a dictionary")
                continue

            # Define default values for all fields
            default_values = {
                "badge": "None",
                "highly_rated": "No",
                "search_term": "",
                "mrp": 0,
                "review_summary": "Not Available",
                "about_this_item": "Not Available",
                "asin": "",
                "last_updated": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                "sponsored": "No",
                "review_text": [],
                "price": 0,
                "bought_recently": "NA",
                "detail_url": "Not Available",
                "all_review_url": "Not Available",
                "rating": 0,
                "name": ""
            }

            # Map both camelCase and snake_case field names
            field_mappings = {
                # Standard fields
                "badge": ["Badge", "badge"],
                "highly_rated": ["HighlyRated", "highly_rated"],
                "search_term": ["SearchTerm", "searchTerm", "search_term"],
                "mrp": ["MRP", "mrp"],
                "review_summary": ["ReviewSummary", "review_summary"],
                "about_this_item": ["AboutThisItem", "about_this_item"],
                "asin": ["ASIN", "asin"],
                "last_updated": ["LastUpdated", "last_updated", "Date"],
                "sponsored": ["Sponsored", "sponsored"],
                "review_text": ["ReviewText", "review_text"],
                "price": ["Price", "price"],
                "bought_recently": ["BoughtRecently", "bought_recently"],
                "detail_url": ["DetailURL", "detail_url"],
                "all_review_url": ["AllReviewURL", "all_review_url"],
                "rating": ["Rating", "rating"],
                "name": ["Name", "name"]
            }

            # Build standardized product dictionary
            standardized_product = {}

            for standard_key, possible_keys in field_mappings.items():
                # Try all possible keys and use the first one found
                value = None
                for key in possible_keys:
                    if key in product:
                        value = product[key]
                        break

                # Use default value if no matching key found
                if value is None:
                    value = default_values[standard_key]
                    print(f"Warning: Using default value for {standard_key} in product {index}")

                # Type conversion and validation
                if standard_key in ['price', 'mrp', 'rating']:
                    try:
                        value = float(value) if value is not None else default_values[standard_key]
                    except (ValueError, TypeError):
                        print(f"Warning: Invalid {standard_key} value in product {index}, using default")
                        value = default_values[standard_key]

                standardized_product[standard_key] = value

            standardized_products.append(standardized_product)

        except Exception as e:
            print(f"Error processing product at index {index}: {str(e)}")
            print(f"Problematic product data: {product}")
            continue

    return standardized_products


def fetch_product_list(search_term):
    base_url = "https://www.amazon.in/s?k={}"
    search_url = base_url.format(search_term.replace(" ", "+"))
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "DNT": "1",  # Do Not Track
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }
    proxies = {
        "http": "http://wxyncgee:qt5rc0jgjlex@198.23.239.134:6540",
        "https": "http://wxyncgee:qt5rc0jgjlex@198.23.239.134:6540"

    }
    response = requests.get(search_url, headers=headers, proxies=proxies)

    # Save the response to debug
    # with open("amazon_page.html", "w", encoding="utf-8") as f:
    #     f.write(response.text)

    if response.status_code != 200:
        return {"error": f"Failed to fetch Amazon page: {response.status_code}"}

    soup = BeautifulSoup(response.content, "html.parser")
    product_data = []

    # Select the highly rated section
    highly_rated_section = soup.select_one("span [data-component-type='s-searchgrid-carousel']")
    highly_rated_products = highly_rated_section.select(".s-result-item") if highly_rated_section else []

    for product in soup.select(".s-main-slot .s-result-item"):

        # Extract Product Name
        name_tag = product.select_one(".a-size-base-plus.a-spacing-none.a-color-base.a-text-normal")
        product_name = name_tag.get_text(strip=True) if name_tag else "Unknown"
        if product_name == "Unknown":
            continue
        else:
            # Check if the product is sponsored
            sponsored_tag = product.select_one(".puis-label-popover")
            if sponsored_tag and 'Sponsored' in sponsored_tag.text:
                sponsored = "Yes"
            else:
                sponsored = "No"

            # Product Detail Page
            link_tag = name_tag.parent if name_tag else None
            link = f"https://www.amazon.in{link_tag['href']}" if link_tag and 'href' in link_tag.attrs else "NA"
            if link == "NA":
                print(f"Skipping product {product_name} due to missing detail url.")
                continue

            # Extract ASIN
            asin = product.get("data-asin", "NA")

            # Get price
            price_tag = product.select_one('.a-price-whole')
            price = float(price_tag.get_text(strip=True).replace(",", "").replace("₹", "")) if price_tag else 0.0

            # Extract MRP from the "a-section aok-inline-block" div
            mrp_div = product.select_one(".a-section.aok-inline-block")
            if mrp_div:
                mrp_value_tag = mrp_div.select_one(".a-price.a-text-price span.a-offscreen")
                if mrp_value_tag:
                    try:
                        mrp = float(mrp_value_tag.text.strip().replace(",", "").replace("₹", ""))
                    except ValueError:
                        mrp = price
                else:
                    mrp = price
            else:
                mrp = price

            # Get rating
            rating_tag = product.select_one('.a-icon-star-small .a-icon-alt')
            rating_text = rating_tag.get_text(strip=True) if rating_tag else "NA"
            rating = float(rating_text.split()[0]) if rating_text != "NA" else 0.0

            # Extract 'how many bought in the past month' text
            bought_recently_tag = product.select_one(".a-row.a-size-base span")
            if bought_recently_tag and 'bought in past month' in bought_recently_tag.text:
                bought_recently = bought_recently_tag.text.strip()
            else:
                bought_recently = "NA"

            # Extract special badge
            badge_tag = product.select_one(".a-badge-text")
            badge = badge_tag.text.strip() if badge_tag else "None"

            # check if the product is part of the highly rated sponsored section
            highly_rated = "Yes" if product in highly_rated_products else "No"

            product_info = {
                "name": product_name,
                "detail_url": link,
                "price": price,
                "mrp": mrp,
                "asin":asin,
                "rating": rating,
                "bought_recently": bought_recently,
                "sponsored": sponsored,
                "badge": badge,
                "highly_rated": highly_rated,
                "review_text": [],  # Initialize empty list
                "about_this_item": "Not Available",  # Initialize with default value
                "all_review_url": "Not Available",  # Initialize with default value
                "review_summary": "Not Available"  # Initialize with default value
            }
            if isinstance(product_info, dict) and "name" in product_info and "detail_url" in product_info:
                product_data.append(product_info)
            else:
                print("Skipping invalid product_info:", product_info)

        if len(product_data) >= 5:  # Limit to first 10 products
            break

    if not isinstance(product_data, list) or not all(isinstance(item, dict) for item in product_data):
        raise ValueError("product_data is not a valid list of dictionaries.")

    return product_data


# Function to save product details to Excel

def save_to_excel(product_details, filename="productresult.xlsx"):
    if not isinstance(product_details, list) or not all(isinstance(item, dict) for item in product_details):
        raise ValueError("Invalid product details. Expected a list of dictionaries.")

    if not product_details:
        print("No product details to save.")
        return

    df = pd.DataFrame(product_details)
    df = df.drop_duplicates(subset=['name', 'detail_url'], keep='first')  # Ensure no duplicates
    df.to_excel(filename, index=False)
    print(f"Data successfully saved to {filename}")


# Function to fetch product details for each product
def fetch_product_detail(product_url):
    # Make an HTTP GET request to fetch the product details page
    response = requests.get(product_url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    })

    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract the "About this item" section
        about_this_item = soup.select_one("#feature-bullets")
        about_text = "\n".join(
            [li.text.strip() for li in about_this_item.select("li")]) if about_this_item else "Not Available"

        # Extract all the individual review text
        review_text_list = []
        review_divs = soup.select("div[data-hook='review-collapsed']")
        for review_div in review_divs:
            # Find the span containing the review text
            span_tag = review_div.find("span")
            if span_tag:
                review_text = span_tag.get_text(strip=True)  # Extract and strip the text
                review_text_list.append(review_text)

        # Extract the See All review Link
        all_reviews_tag = soup.select_one("a[data-hook='see-all-reviews-link-foot']")
        all_reviews_url = f"https://www.amazon.in{all_reviews_tag['href']}" if all_reviews_tag else "Not Available"

        # Extract the "Customer say" section
        customer_say_section = soup.select_one("#product-summary")
        if customer_say_section:
            # Extract the main insights text
            review_summary = customer_say_section.select_one(
                "p span").text.strip() if customer_say_section.select_one("p span") else "Not Available"

        else:
            review_summary = "Not Available"

        # print(review_text_list)
        return {
            "url": product_url,
            "status": "Success",
            "about_this_item": about_text,
            "review_text": review_text_list,
            "all_review_url": all_reviews_url,
            "review_summary": review_summary
        }
    else:
        return {
            "url": product_url,
            "status": f"Failed - HTTP {response.status_code}",
            "about_this_item": None,
            "review_text": None,
            "all_review_url": None,
            "review_summary": None
        }

def fetch_product_details_parallel(product_urls):
    """
    Fetch product details in parallel for a list of URLs.
    """
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_product_detail, url) for url in product_urls]
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error fetching product detail: {e}")
    return results

def store_to_dynamodb(product_data, search_term):
    # Connect to DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('AmazonProducts')  # Replace with your table name

    # Insert each product into DynamoDB
    for product in product_data:
        # Add search term and date
        product["SearchTerm"] = search_term
        product["Date"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # Ensure ASIN is available
        asin = product.get("asin", None)
        if not asin or asin == "NA":
            print(f"Skipping product without valid ASIN: {product}")
            continue
        if not product.get("SearchTerm"):
            print(f"Missing SearchTerm for product: {product}")
            continue

        # Convert float values to Decimal
        item = {
            "ASIN": product["asin"],  # Use ASIN as the primary key
            "SearchTerm": product["SearchTerm"], #Sort key
            "Name": product["name"],
            "Price": Decimal(str(product["price"])) if isinstance(product["price"], float) else product["price"],
            "MRP": Decimal(str(product["mrp"])) if isinstance(product["mrp"], float) else product["mrp"],
            "Rating": Decimal(str(product["rating"])) if isinstance(product["rating"], float) else product["rating"],
            "BoughtRecently": product["bought_recently"],
            "Sponsored": product["sponsored"],
            "HighlyRated": product["highly_rated"],
            "Badge": product["badge"],
            "DetailURL": product.get("detail_url", "NA"),  # Save detail_url as a non-key attribute
            "AboutThisItem": product.get("about_this_item", "NA"),
            "ReviewText": product.get("review_text", []),
            "AllReviewURL": product.get("all_review_url", "NA"),
            "ReviewSummary": product.get("review_summary", "NA"),
            "LastUpdated": product["Date"],
        }
        # Store in dynamo DB
        try:
            table.put_item(Item=item)
        except Exception as e:
            print(f"Error storing product in DynamoDB: {e}")


def fetch_cached_results(search_term):
    """
    Fetch cached results from DynamoDB if the search term exists.

    Args:
        search_term (str): The search term to query the database for.

    Returns:
        list: A list of matching items from DynamoDB.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('AmazonProducts')

    try:
        # Logging the query parameters
        print(f"Fetching cached results for search term: {search_term}")

        # Query using the primary key (ASIN) and sort key (SearchTerm)
        response = table.scan(
            FilterExpression=Attr("SearchTerm").eq(search_term)
        )

        items = response.get("Items", [])

        # Log the number of items fetched
        if items:
            print(f"Fetched {len(items)} items from DynamoDB for search term '{search_term}'.")
        else:
            print(f"No items found in DynamoDB for search term '{search_term}'.")

        # Log the fetched items for debugging (optional)
        print(f"Fetched items: {items}")

        return items

    except Exception as e:
        # Log the error with detailed information
        print(f"Error fetching cached results for search term '{search_term}': {e}")
        return []



def upload_to_s3(file_path, bucket_name, object_name):
    """
    Uploads a file from the /tmp directory to an S3 bucket.
    Args:
        file_path (str): Path to the file in the Lambda /tmp directory.
        bucket_name (str): Name of the S3 bucket.
        object_name (str): Name of the file in the S3 bucket.
    """
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


# Call the main function
def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Args:
    event: Dictionary containing input data. (e.g., {"searchTerm": "drink coasters"})
    context: AWS Lambda context object.
    Returns:
    dict: Status of execution.
    """
    # Log the event received
    print("Event received:", json.dumps(event, indent=2))

    # Try to parse the searchTerm
    try:

        body = json.loads(event.get("body", "{}"))
        search_term = body.get("searchTerm", None)

        if not search_term:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"message": "searchTerm is required in the request body"}),
            }

        print(f"Search Term: {search_term}")

        # Check cache
        cached_results = fetch_cached_results(search_term)
        if cached_results:
            # Convert cached results to JSON-serializable format
            for item in cached_results:
                for key, value in item.items():
                    if isinstance(value, Decimal):
                        item[key] = float(value)  # Convert Decimal to float

            print(f"Returning cached results for search term '{search_term}'")
            normalized_cached_results = normalize_response(cached_results)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "message": "Cached results found",
                    "s3_file_url": f"https://amazon-frontend-test.s3.ap-south-1.amazonaws.com/index.html",
                    "product_details": normalized_cached_results,
                }),
            }

        # Execute product list fetching
        product_details = fetch_product_list(search_term)

        # print(product_details)

        if not product_details:
            # print("No products found for the given search term. Skipping further processing.")
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",  # For CORS
                },
                "body": json.dumps({
                    "message": "No products found.",
                }),
            }
        # Extract detail URLs for parallel processing
        product_urls = [product["detail_url"] for product in product_details if product["detail_url"] != "NA"]
        detailed_results = fetch_product_details_parallel(product_urls)

        print(detailed_results)

        # Update product_details with detailed info
        for product in product_details:
            matching_details = next(
                (details for details in detailed_results if details["url"] == product["detail_url"]),
                {
                    "about_this_item": "NA",
                    "review_text": [],
                    "all_review_url": "NA",
                    "review_summary": "NA"
                }
            )

            # Preserve existing fields while updating with new details
            product.update({
                "about_this_item": matching_details.get("about_this_item", "NA"),
                "review_text": matching_details.get("review_text", []),
                "all_review_url": matching_details.get("all_review_url", "NA"),
                "review_summary": matching_details.get("review_summary", "NA")
            })

        # Normalize dynamically fetched results
        normalized_product_details = normalize_response(product_details)

        # Save to DynamoDB
        store_to_dynamodb(normalized_product_details, search_term)

        # Save to Excel file in /tmp (Lambda's temp storage)
        output_file = "/tmp/product_results.xlsx"
        save_to_excel(normalized_product_details, filename=output_file)

        # Upload to S3 bucket
        bucket_name = "my-lambda-output"  # Replace with your bucket name
        object_name = "product_results.xlsx"
        upload_to_s3(output_file, bucket_name, object_name)

        # Return results to the frontend
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "message": "Execution completed successfully.",
                "s3_file_url": f"https://amazon-frontend-test.s3.ap-south-1.amazonaws.com/index.html",
                "product_details": normalized_product_details,
            }),
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": "An unexpected error occurred", "error": str(e)}),
        }

