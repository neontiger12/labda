import boto3
from datetime import datetime

#Get today's date
today = datetime.today()

#Format as string: YYY/MM/DD
todays_date = today.strftime("%Y%m%d")

#print(todays_date)

#Connect with S3


session = boto3.Session(profile_name='labda-access')

#check the profile used for the connection
#print(f"Using AWS profile: {session.profile_name}")

s3_client = boto3.client("s3")
bucket_name = "labda-test-001"
response = s3_client.list_buckets()

#list the objects in the bucket


list_objects_response = s3_client.list_objects_v2(Bucket=bucket_name)

# get the 'Contents'

get_contents = list_objects_response.get("Contents")

# get the name of each content element and store it in a variable

get_all_s3_objects_and_folder_names = []

for item in get_contents:
    s3_object_name = item.get("Key")
    get_all_s3_objects_and_folder_names.append(s3_object_name)

#Check the items in the bucket
#print(get_all_s3_objects_and_folder_names)
    
#Create the folder if it doesn't exists
    
directory_name = todays_date + "/"

#Check if the todays directory already exists

if directory_name not in get_all_s3_objects_and_folder_names:
    s3_client.put_object(Bucket=bucket_name, Key=(directory_name))

#Put the object that was uploaded today to the today's folder

#check:
#1. if the last modified date for the object match our folder name
#2. make sure the objects we are moving doesn't have the / at the end

# delete object that we already moved

    

for item in get_contents:
    object_creation_date = item.get("LastModified").strftime("%Y%m%d") + "/"
    object_name = item.get("Key")
   

    if object_creation_date == directory_name and "/" not in object_name:
        s3_client.copy_object(Bucket=bucket_name, CopySource=bucket_name +"/"+object_name, Key=directory_name+object_name)
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)


        


