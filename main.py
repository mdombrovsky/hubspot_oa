import requests
import json


def calculateSession(data):
    events = data['events']
    users = {}
    userSessions = {}
    result = {'sessionsByUser': {}}

    # allocate each event to a user
    for event in events:
        id = event['visitorId']
        if id not in users:
            users[id] = [event]
            userSessions[id] = []
            result['sessionsByUser'][id] = []
        else:
            users[id].append(event)

    for user in users:
        # sort every event a user has
        users[user].sort(key=lambda x: x['timestamp'])

        # aggregate events into sessions
        for event in users[user]:
            url = event['url']
            timestamp = event['timestamp']

            # userSessions:{
            #  user: [[event1, event2],[event3, event4],[event5]]
            # }
            # if its empty or previous session is greater than 10 minutes earlier, create new one
            if len(userSessions[user]) == 0 or timestamp - userSessions[user][-1][-1]['timestamp'] > 600000:
                userSessions[user].append([event])
            # otherwise, add it to existing event
            else:
                userSessions[user][-1].append(event)

        # format data
        for session in userSessions[user]:
            startTime = session[0]['timestamp']
            endTime = session[-1]['timestamp']
            pages = []
            for event in session:
                pages.append(event['url'])

            entry = {
                'duration': endTime - startTime,
                'pages': pages,
                'startTime': startTime
            }

            result['sessionsByUser'][user].append(entry)

    return result


get_request_url = 'https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey=redacted'
post_request_url = 'https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey=redacted'
get_response = requests.get(get_request_url)
if get_response.status_code == 200:
    print("Successfully received data")
    data = get_response.json()
    result = calculateSession(data)
    json_result = json.dumps(result)
    post_response = requests.post(post_request_url, data=json_result)

    if post_response.status_code == 200:
        print("Successfully sent data")
    else:
        print("Error sending data, code " + str(post_response.status_code))
else:
    print("Error receiving data")
