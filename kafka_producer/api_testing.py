from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/testing', methods=['POST'])
def testing():
    json_data = request.get_json()
    raw_data = request.data
    print('json data: ', json_data)
    print('raw data: ', raw_data)
    return jsonify({'message': 'ahihi', 'status': 'Pass'})

if __name__ == '__main__':
    app.run(debug=True, host='192.168.0.100', port=5000)
