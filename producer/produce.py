import random, time, boto3
from datetime import datetime
#http 2023-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 3.3.3.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/ HTTP/1.1" "curl/7.46.0" - - arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2019-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-"

def generate_log_line():
    type = random.choice(['http', 'https'])
    # timestamp
    elb_name = 'app/my-loadbalancer/50dc6c495c0c9188'
    client_ip = f'{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}:{random.randint(0,65525)}'
    backend_ip = random.choice([
        '10.0.0.1:8081',
        '10.0.0.2:8080',
    ])

    request_processing_time = random.uniform(0, 3)
    backend_processing_time = random.uniform(0, 2)
    response_processing_time = random.uniform(0, 1)
    elb_status_codes = random.choice([ 200, 201, 204, 400, 401, 404, 500, 503 ])
    backend_status_code = random.choice([ 200, 201, 204, 400, 401, 404, 500, 503 ])
    received_bytes = random.randint(0, 1000)
    sent_bytes = random.randint(0, 30000)

    request_method = random.choice(['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
    request_uri = type + '://example.com' + random.choice(['/product', '/user', '/admin', '/sale'])
    request_protocol = random.choice(['HTTP/1.1', 'HTTP/1.1', 'HTTP/2.0'])
    request = f'"{request_method} {request_uri} {request_protocol}"'

    user_agent = random.choice([
        '"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"',
        '"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"',
        '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"',
        '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"',
    ])

    ssl_cipher = random.choice(['"-"' ,'ECDHE-RSA-AES128-GCM-SHA256'])
    ssl_protocol = random.choice(['"-"', 'TLSv1.2'])

    target_group_arn = random.choice([
        'arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/target1/73e2d6bc24d8a067',
        'arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/target2/73e2d6bc24d8a067',                                  
    ])

    the_rest = '"Root=1-58337262-36d228ad5d99923122bbe354" "-" "-" 0 2019-07-02T22:22:48.364000Z "forward" "-" "-" "10.0.0.1:80" "200" "-" "-"'

    current_time = datetime.now()
    timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    random_log = f'{type} {timestamp} {elb_name} {client_ip} {backend_ip} {request_processing_time} {backend_processing_time} {response_processing_time} {elb_status_codes} {backend_status_code} {received_bytes} {sent_bytes} {request} {user_agent} {ssl_cipher} {ssl_protocol} {target_group_arn} {the_rest}'

    return random_log

s3 = boto3.client('s3')

while True:
    try:
        logs = generate_log_line() + '\n' + \
                generate_log_line() + '\n' + \
                generate_log_line()

        current_time = datetime.now()
        timestamp = current_time.strftime('%Y-%m-%dT%H:%M:%S')
        file_key = f'input/aws/{timestamp}.log'
        bucket_name = 'ratko-test-bucket'
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=logs)
        print(f"String saved to S3 bucket '{bucket_name}' as '{file_key}'")
    except Exception as e:
        print(f"Error saving string to S3: {e}")

    time.sleep(60)