import requests
import boto3
import json
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import logging


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

        # # Log the fetched items for debugging (optional)
        # print(f"Fetched items: {items}")

        return items

    except Exception as e:
        # Log the error with detailed information
        print(f"Error fetching cached results for search term '{search_term}': {e}")
        return []


def validate_deepseek_response(response):
    """
    Validate the DeepSeek API response structure.

    Args:
        insights (dict): The JSON response from DeepSeek API.

    Returns:
        bool: True if the response is valid, otherwise raises a ValueError.
    """
    # Define the required keys and their expected structure
    try:
        choices = response.get("choices", [])
        if not choices:
            raise ValueError("Missing 'choices' in DeepSeek response.")

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            raise ValueError("Missing 'content' in DeepSeek response.")

        insights = json.loads(content)  # Parse the JSON string in 'content'

        required_keys = [
            "recommended_title",
            "recommended_description",
            "identified_gaps",
            "messaging_positioning",
            "opportunity_size"
        ]
        for key in required_keys:
            if key not in insights:
                raise ValueError(f"Missing required field: {key}")

        # Additional validations for nested structures
        if not isinstance(insights["identified_gaps"], dict):
            raise ValueError("Invalid format for 'identified_gaps'.")
        if not isinstance(insights["messaging_positioning"], dict):
            raise ValueError("Invalid format for 'messaging_positioning'.")
        if not isinstance(insights["opportunity_size"], dict):
            raise ValueError("Invalid format for 'opportunity_size'.")

        return True
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON structure in 'content'.")
    except Exception as e:
        raise ValueError(f"Error validating DeepSeek response: {e}")

def store_deepseek_response(deepseek_response):
    """
    Process and store the relevant part of the DeepSeek response in 'insights'.

    Args:
        deepseek_response (dict): The raw DeepSeek API response.

    Returns:
        dict: The parsed and stored insights.
    """
    try:
        choices = deepseek_response.get("choices", [])
        if not choices:
            raise ValueError("Missing 'choices' in DeepSeek response.")

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            raise ValueError("Missing 'content' in DeepSeek response.")

        insights = json.loads(content)  # Parse the content JSON string
        return insights
    except Exception as e:
        raise ValueError(f"Error storing DeepSeek response: {e}")

def handle_get_insights_request(search_term):
    """
    Handle the request to fetch insights for a search term by calling the DeepSeek R1 API.

    Args:
        search_term (str): The search term for which insights are requested.

    Returns:
        dict: A dictionary containing the structured insights.
    """
    try:
        # Step 1: Query DynamoDB for product details related to the search term
        try:
            cached_results = fetch_cached_results(search_term)
        except Exception as db_error:
            return {"error": f"Failed to fetch cached results: {str(db_error)}"}

        if not isinstance(cached_results, list) or not all(isinstance(item, dict) for item in cached_results):
            return {"error": "Invalid data format in cached results. Expected a list of dictionaries."}

        if not cached_results:
            return {"error": f"No cached results found for search term: {search_term}"}

        # print(f"Fetched {len(cached_results)} items from DynamoDB for search term: {search_term}")

        # Step 2: Format data for DeepSeek R1 API
        product_data = []
        for item in cached_results:
            product_data.append({
                "name": item.get("Name", "N/A"),
                "price": float(item.get("Price", 0)),
                "mrp": float(item.get("MRP", 0)),
                "rating": float(item.get("Rating", 0)),
                "boughtRecently": item.get("BoughtRecently", "N/A"),
                "badge": item.get("Badge", "None"),
                "sponsored": item.get("Sponsored", "No"),
                "highlyRated": item.get("HighlyRated", "No"),
                "aboutThisItem": item.get("AboutThisItem", "N/A"),
                "reviewSummary": item.get("ReviewSummary", "N/A"),
                "reviewText": item.get("ReviewText", [])
            })

        # Step 3: Call the DeepSeek R1 API
        deepseek_api_url = "https://api.deepseek.com/v1/chat/completions"
        
        if not deepseek_api_key:
            return {"error": "DeepSeek API key is missing. Please configure it as an environment variable."}

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {deepseek_api_key}"}

        instructions = """
        You are an expert in Amazon e-commerce and have been provided with a dataset of product listings for the search term "wood coasters." Your task is to analyze this dataset to help a brand list a new product for the same search term on Amazon. The brand is seeking detailed insights to optimize their product listing. Please follow the instructions below to perform a thorough analysis:

        1. **Recommended Final Product Title**:
           - Analyze the existing product titles in the dataset.
           - Identify common patterns, keywords, and structures.
           - Propose a compelling and SEO-friendly product title that stands out while incorporating relevant keywords.

        2. **Recommended Final Product Description for "About Item" Section**:
           - Review the "AboutThisItem" sections of the top-performing products.
           - Identify key features, benefits, and unique selling points (USPs) that resonate with customers.
           - Craft a detailed and engaging product description that highlights the product's features, benefits, and USPs.

        3. **Gaps Within the Current Products Listed for the Keyword**:
           - Analyze customer reviews (both positive and negative) to identify common complaints and unmet needs.
           - Compare the features, quality, and pricing of existing products.
           - Highlight gaps in the market that the new product can address.

        4. **Recommended Product Messaging Positioning**:
           - Based on the identified gaps, suggest how the new product can be positioned in the market.
           - Recommend key messaging points that emphasize the product's unique features and benefits.
           - Suggest how to differentiate the product from competitors.

        5. **Expected Opportunity Size Based on Purchase Trends**:
           - Analyze the "BoughtRecently" and "Rating" columns to understand purchase trends and customer satisfaction.
           - Estimate the potential market size and opportunity for the new product.
           - Provide insights into pricing strategies based on the competition.All prices are in INR.

        **Output Format**:
        The output should be in JSON format, structured as follows:

        ```json
        {
          "recommended_title": "Recommended product title",
          "recommended_description": "Recommended product description",
          "identified_gaps": {
            "gap_1": "Description of the first gap",
            "gap_2": "Description of the second gap",
            "gap_3": "Description of the third gap"
          },
          "messaging_positioning": {
            "key_message_1": "First key messaging point",
            "key_message_2": "Second key messaging point",
            "key_message_3": "Third key messaging point"
          },
          "opportunity_size": {
            "market_size_estimate": "Estimated market size",
            "pricing_strategy": "Recommended pricing strategy"
          }
        }
        Do not give output in any other format. If you do not have the results in the above format, return a short text explanation on why insights could not be structured in the above specified json format

        """

        # Define the payload with your data, instructions, and output format
        payload = {
            "model": "deepseek-chat",  # Specify the model (e.g., deepseek-r1)
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user",
                 "content": "Analyze the following product data and provide the output in JSON format."},
                {"role": "assistant", "content": "Sure! Please provide the data and any specific instructions."},
                {"role": "user",
                 "content": f"Data: {json.dumps(product_data)}. Instructions: {instructions}"}
            ],
            "response_format": {"type": "json_object"}  # Request structured JSON output
        }

        # Make the API request
        response = requests.post(deepseek_api_url, headers=headers, json=payload, timeout=30)

        if response.status_code != 200:
            return {
                "error": f"DeepSeek API call failed with status {response.status_code}: {response.text}"
            }

        # Log the response body
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)  # Use DEBUG for more granular logs
        logger.info(f"DeepSeek API Response: {response.text}")

        # Step 4: Parse the API response
        insights = store_deepseek_response(response.json())

        # Validate the response structure
        validate_deepseek_response(response.json())

        return {"success": True, "insights": insights}

    except Exception as e:
        print(f"Error in get insights: {str(e)}")
        return {"error": f"Failed to fetch insights: {str(e)}"}


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
        search_term = body.get("searchTerm")

        if not search_term:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"message": "searchTerm is required for insights"}),
            }

        # Call the function to get insights
        insights_response = handle_get_insights_request(search_term)

        # Return insights results
        return {
            "statusCode": 200 if "success" in insights_response else 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(insights_response),
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": "An unexpected error occurred", "error": str(e)}),
        }
