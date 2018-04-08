import os
import AwsLambda


os.environ['AVROTABLENAME'] = 'users.avro'

AwsLambda.lambda_handler(None,None)