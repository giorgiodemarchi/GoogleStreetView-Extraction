import pandas as pd
import boto3 
from io import StringIO, BytesIO
import requests
from PIL import Image 
import io
import math
import os 

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Check if in dataset
def get_folder_names(directory_name, bucket_name = 'detroit-project-data-bucket'):
    """
    Connect to S3 and read all folder (datapoints) names in the images dataset
    """

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=directory_name,
        Delimiter='/'
    )

    folder_names = []
    for response in response_iterator:
        if response.get('CommonPrefixes') is not None:
            for prefix in response.get('CommonPrefixes'):
                # Extract the folder name from the prefix key
                folder_name = prefix.get('Prefix')
                # Removing the base directory and the trailing slash to get the folder name
                folder_name = folder_name[len(directory_name):].strip('/')
                folder_names.append(folder_name)

    return folder_names

def already_in_dataset(coordinates, directory_name):
    """
    Check if coordinate point is already in dataset

    Input: Coordinates (lat, lon) --Tuple
    Output: Boolean TRUE/FALSE
    
    """
    coords_stored = []
    items = get_folder_names(directory_name) 
    if len(items)>0:
        for item in items:
            lat = item.split("_")[2]
            lon = item.split("_")[3]
            coords_stored.append((lat, lon))
        if coordinates in coords_stored:
            return 1, len(items)
    return 0, len(items)

# Extract images
def get_street_view_images(api_key, location, size, headings, pitch=0, fov=90):
    """
    API Call to get Street View images
    
    Cost per 1000 requests: $7
    """
    images = []

    base_url = "https://maps.googleapis.com/maps/api/streetview"
    for heading in headings:
        params = {
            "key": api_key,
            "location": f"{location[0]},{location[1]}",
            "size": size,
            "heading": heading,
            "pitch": pitch,
            "fov": fov,
            "source": "outdoor"
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            image_data = io.BytesIO(response.content)
            images.append(Image.open(image_data))
        else:
            print(f'Error with Google API at location {location} - status code: {response.status_code}')
            # raise ValueError(f'Error with Google API at location {location} - status code: {response.status_code}')
    return images

# A few functions that are necessary to handle angles
def normalize_angle(angle):
    """
    Normalize the angle to be within [0, 360) degrees.
    """
    normalized_angle = angle % 360
    return normalized_angle
def add_angles(angle1, angle2):
    """
    Add two angles, considering the circular nature of angles.
    """
    result = normalize_angle(angle1 + angle2)
    return result
def subtract_angles(angle1, angle2):
    """
    Subtract one angle from another, considering the circular nature of angles.
    """
    result = normalize_angle(angle1 - angle2)
    return result
def calculate_orientation(coord1, coord2):
    """
    Compute N-E-S-W Orientation of the street based on two points belonging in it.
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    Δλ = lon2 - lon1
    Δφ = lat2 - lat1
    # Calculate the angle in radians
    θ = math.atan2(Δλ, Δφ)
    # Convert angle to degrees
    θ_deg = θ * (180.0 / math.pi)
    # Normalize the angle to be between 0 and 360 degrees
    normalized_angle = (θ_deg + 360) % 360
    # Determine cardinal direction
    cardinal_directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    index = int((normalized_angle + 22.5) / 45) % 8
    direction = cardinal_directions[index]
    # Determine the angle at which the street is directed (using the API standards)
    angle_google_api = [0, 45, 90, 135, 180, 225, 270, 315]
    baseline_angle = angle_google_api[index]
    return direction, baseline_angle

# Save images
def save_data(images, metadata, directory_name, bucket_name = 'detroit-project-data-bucket'):
    """
    Function to store the images
    """
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    angle = metadata['angle']
    p = metadata['p']
    latitude = metadata['latitude']
    longitude = metadata['longitude']
    address = metadata['address']
    headings = metadata['headings']

    # Save the observation of first side (images + metadata)
    datapoint_id = f"{p}_{angle}_{latitude}_{longitude}"
    base_s3_path = f"{directory_name}{datapoint_id}"

    for idx, img in enumerate(images):
        # Convert the image to bytes
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()

        # Create a full path for the image
        img_path = f"{base_s3_path}/image_{idx}.png"

        # Upload the image to S3
        s3_client.put_object(Bucket=bucket_name, Key=img_path, Body=img_byte_arr)

    # Prepare metadata text
    metadata_content = f"""Latitude: {latitude}
Longitude: {longitude}
Headings: {headings}
Address: {address}"""

    # Save Metadata to a .txt file
    metadata_path = f"{base_s3_path}/metadata.txt"
    s3_client.put_object(Bucket=bucket_name, Key=metadata_path, Body=metadata_content)
