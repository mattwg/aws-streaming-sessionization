import base64

payload = "{'page_name': 'homepage', 'hit_timestamp': '2019-01-01T00:03:23', 'visitor_id': 21418, 'device_id': 'tablet'}{'page_name': 'registration_success', 'hit_timestamp': '2019-01-01T00:03:25', 'visitor_id': 19526, 'device_id': 'mobile'}{'page_name': 'signin', 'hit_timestamp': '2019-01-01T00:03:36', 'visitor_id': 19526, 'device_id': 'mobile'}{'page_name': 'signin', 'hit_timestamp': '2019-01-01T00:03:40', 'visitor_id': 19526, 'device_id': 'mobile'}{'page_name': 'homepage', 'hit_timestamp': '2019-01-01T00:03:44', 'visitor_id': 19526, 'device_id': 'mobile'}"

print (base64.b64encode(payload))
