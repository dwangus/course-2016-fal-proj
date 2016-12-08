import jsonschemaimport json
import dml
import requests
from flask import Flask, jsonify, abort, make_response, request
from flask.ext.httpauth import HTTPBasicAuth

app = Flask(__name__)
auth = HTTPBasicAuth()

CLIENT_SIDE_URL = "http://127.0.0.1"
PORT = 8080

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(retrieveData.contributor, retrieveData.contributor)

'''
users = [
    {'id': 1, 'username': u'alice'},
    {'id': 2, 'username': u'bob'}
]
#'''

schema = {
    "type": "object",
    "properties": {"username": {"type": "string"}},
    "required": ["username"]
}

read_files = ['auth.json', 'client_secrets.json', 'etc.txt', 'theseAreAllFakeFiles.txt']
write_filenames = []

for file in read_files:
    with open(file) as data_file:
        if file == read_files[0]:
            auth = json.load(data_file)

        if file == read_files[0]:
            client_secr = json.load(data_file)

        if file == read_files[0]:
            auth = json.load(data_file)

        else:
            auth = json.load(data_file)


@app.route('/client', methods=['OPTIONS'])
def show_api():
    return jsonify(schema)


@app.route('/client', methods=['GET'])
@auth.login_required
def show_client():
    return open('client.html', 'r').read()


@app.route('/app/api/v0.1/users', methods=['GET'])
def get_users():  # Server-side reusable name for function.
    print("I'm responding.")
    return jsonify({'users': users})


@app.route('/app/api/v0.1/users/', methods=['GET'])
def get_user(user_id):
    user = [user for user in users if user['id'] == user_id]
    if len(user) == 0:
        abort(404)
    return jsonify({'user': user[0]})


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found.'}), 404)


@app.route('/app/api/v0.1/users', methods=['POST'])
def create_user():
    print(request.json)
    if not request.json:
        print('Request not valid JSON.')
        abort(400)

    try:
        jsonschema.validate(request.json, schema)
        user = {'id': users[-1]['id'] + 1, 'username': request.json['username']}
        users.append(user)
        print(users)
        return jsonify({'user': user}), 201
    except:
        print('Request does not follow schema.')
        abort(400)


@auth.get_password
def foo(username):
    if username == 'alice':
        return 'ecila'
    return None


@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access.'}), 401)


if __name__ == '__main__':
    app.run(debug=True)

'''
import jsonschema
from flask import Flask, jsonify, abort, make_response, request
from flask.ext.httpauth import HTTPBasicAuth


app = Flask(__name__)
auth = HTTPBasicAuth()

users = [
  { 'id': 1, 'username': u'alice' },
  { 'id': 2, 'username': u'bob' }
]

schema = {
  "type": "object",
  "properties": {"username" : {"type": "string"}},
  "required": ["username"]
}

@app.route('/client', methods=['OPTIONS'])
def show_api():
    return jsonify(schema)

@app.route('/client', methods=['GET'])
@auth.login_required
def show_client():
    return open('client.html','r').read()

@app.route('/app/api/v0.1/users', methods=['GET'])
def get_users(): # Server-side reusable name for function.
    print("I'm responding.")
    return jsonify({'users': users})

@app.route('/app/api/v0.1/users/', methods=['GET'])
def get_user(user_id):
    user = [user for user in users if user['id'] == user_id]
    if len(user) == 0:
        abort(404)
    return jsonify({'user': user[0]})

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found foo.'}), 404)

@app.route('/app/api/v0.1/users', methods=['POST'])
def create_user():
    print(request.json)
    if not request.json:
        print('Request not valid JSON.')
        abort(400)

    try:
        jsonschema.validate(request.json, schema)
        user = { 'id': users[-1]['id'] + 1, 'username': request.json['username'] }
        users.append(user)
        print(users)
        return jsonify({'user': user}), 201
    except:
        print('Request does not follow schema.')
        abort(400)

@auth.get_password
def foo(username):
    if username == 'alice':
        return 'ecila'
    return None

@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access.'}), 401)

if __name__ == '__main__':
    app.run(debug=True)

'''