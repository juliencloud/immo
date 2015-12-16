import boto3


s3 = boto3.resource('s3')


def get_bucket(settings):
    return s3.Bucket(settings['s3']['bucket_thumbs'])


def write_file(bucket, path, content):
    bucket.put_object(Key=path, ACL='private', Body=content)
