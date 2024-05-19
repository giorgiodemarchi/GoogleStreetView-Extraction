import pandas as pd
import os

from utils import (get_street_view_images, 
                   add_angles, subtract_angles, calculate_orientation,
                   already_in_dataset, save_data
)

def generate_images(points_df, point_id, api_key, image_size, directory_name):
    """ 
    Return 5 images
    """
    p = point_id
    coordinates = (points_df.loc[points_df['point_id']==p,'latitude'].item(), points_df.loc[points_df['point_id']==p,'longitude'].item())

    ###  Set headings
    # Get a second point on the same line and compute direction
    second_point = points_df[(points_df["street_id"] == points_df.loc[points_df['point_id']==p,"street_id"].item())
                            & (points_df["point_id"] != p)].iloc[0]
    second_point_coordinates = (second_point["latitude"], second_point["longitude"])
    _, angle_baseline = calculate_orientation(coordinates, second_point_coordinates)
    
    ## FIRST SIDE
    ### Do it for one side of the street
    straight_one = angle_baseline + 90
    headings_one = [subtract_angles(straight_one, 60), subtract_angles(straight_one, 30), straight_one, add_angles(straight_one, 30), add_angles(straight_one, 60)] 
    street_view_images_first = get_street_view_images(api_key, coordinates, image_size, headings_one)
    metadata_one = {'p':p, 
                    'angle':straight_one,
                    'latitude': coordinates[0],
                    'longitude': coordinates[1],
                    'headings': headings_one,
                    'address': 'N/A'} # address}
    
    # SECOND SIDE
    straight_two = angle_baseline - 90
    headings_two = [subtract_angles(straight_two, 60), subtract_angles(straight_two, 30),  straight_two,  add_angles(straight_two, 30), add_angles(straight_two, 60)] 
    street_view_images_second = get_street_view_images(api_key, coordinates, image_size, headings_two)
    metadata_two = {'p':p, 
            'angle':straight_two,
            'latitude': coordinates[0],
            'longitude': coordinates[1],
            'headings': headings_two,
            'address': 'N/A'} # address}

    return [street_view_images_first, street_view_images_second], [metadata_one, metadata_two]

def update_tracking_csv(point, len_points):
    """
    Keep tracks on what is in dataset and how many images
    """
    tracking_df = pd.read_csv('pipe_tracking.csv', index_col=0)
    tracking_df.loc[tracking_df['point_id'] == point, 'Images'] = len_points
    tracking_df.loc[tracking_df['point_id'] == point, 'In Dataset'] = 1
    tracking_df.to_csv('pipe_tracking.csv')

def print_status(i):
    """
    Print status of pipeline on terminal
    """
    print("-----------------------")
    print(f"- Iteration of the run: {i}")
    tracking_df = pd.read_csv('pipe_tracking.csv', index_col=0)
    print(f"In dataset: {tracking_df['In Dataset'].sum()}")
    print(f"Broken points in dataset: {len(tracking_df[tracking_df['Images'].isin([0,1,2,3,4,5,6,7,8,9])])}")
    print(f"Remaining to upload: {tracking_df['Images'].isnull().sum()} ")

if __name__=='__main__':
    IMAGE_SIZE = "640x480"
    API_KEY = os.environ.get('GOOGLE_API_KEY')
    DIRECTORY_NAME = 'GoogleDetroitDatabase/'

    # df = pd.read_csv('../Data/LocationSamplingDataset/FullDetroitPointsDataset_v2.csv', index_col=0)[['point_id', 'street_id', 'longitude', 'latitude']]
    
    df = pd.read_csv('pipe_tracking.csv', index_col=0)

    points_ids = df[(df['In Dataset']!=1) & (df['points_in_street']!=1)].point_id.unique()

    i=0
    for point in points_ids:
        print(point)
        i+=1
        data_points, metadata = generate_images(df, point, API_KEY, IMAGE_SIZE, DIRECTORY_NAME)
        
        if data_points is not None and len(data_points[0])==5 and len(data_points[1])==5:
            # SAVE
            save_data(data_points[0], metadata[0], DIRECTORY_NAME)
            save_data(data_points[1], metadata[1], DIRECTORY_NAME) 
            
            ## Update pipeline tracking dataframe (store as csv)
            len_points = len(data_points[0]) + len(data_points[1])
            update_tracking_csv(point, len_points)
        else:
            # raise ValueError(f'Less than 5 images returned for point {point}')
            print(f'Less than 5 images returned for point {point}')

        if i%50==0:
            print_status(i)

        if i == 20000:
            break
