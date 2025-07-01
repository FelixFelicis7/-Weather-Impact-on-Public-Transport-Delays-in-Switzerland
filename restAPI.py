from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

# This file contains the implementation of the REST API.
# With the REST API one is able to access the weatherstation table.
# The user can then use GET/POST/PUT/DELETE to interact with the table through the REST API.
# To interact with it, one needs to run the main function of this script and then use a command
# in the terminal with similar structure as this 'curl -X GET http://127.0.0.1:5000/weatherstation'.
# This command would return the user the information on all weather stations.

app = Flask(__name__)

# Database Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:admin@localhost:5432/weather_transport_data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


# Define the WeatherStation Model
class WeatherStation(db.Model):
    __tablename__ = 'weatherstation'
    weatherstationname = db.Column(db.String(30), primary_key=True)
    canton = db.Column(db.String(30))
    station = db.Column(db.String(30))
    wigosid = db.Column(db.String(30))
    datasince = db.Column(db.Date)
    stationheight = db.Column(db.Float)
    coorde = db.Column(db.Float)
    coordn = db.Column(db.Float)
    lat = db.Column(db.Float)
    long = db.Column(db.Float)
    climateregion = db.Column(db.String(40))


# GET: Retrieve all weather stations
# command: curl -X GET http://127.0.0.1:5000/weatherstation
@app.route('/weatherstation', methods=['GET'])
def get_weatherstations():
    stations = WeatherStation.query.all()
    return jsonify([
        {
            "WeatherStationName": station.weatherstationname,
            "Canton": station.canton,
            "Station": station.station,
            "WIGOSID": station.wigosid,
            "DataSince": station.datasince,
            "StationHeight": station.stationheight,
            "CoordE": station.coorde,
            "CoordN": station.coordn,
            "Lat": station.lat,
            "Long": station.long,
            "ClimateRegion": station.climateregion,
        } for station in stations
    ])


# GET: Retrieve a specific weather station by name
# Here one can use the same command as before, just with an added weather station name.
# command: curl -X GET http://127.0.0.1:5000/weatherstation/XXX
@app.route('/weatherstation/<weatherstation_name>', methods=['GET'])
def get_weatherstation(weatherstation_name):
    station = WeatherStation.query.get(weatherstation_name)
    if not station:
        return jsonify({"error": f"WeatherStation '{weatherstation_name}' not found"}), 404
    return jsonify({
        "WeatherStationName": station.weatherstationname,
        "Canton": station.canton,
        "Station": station.station,
        "WIGOSID": station.wigosid,
        "DataSince": station.datasince,
        "StationHeight": station.stationheight,
        "CoordE": station.coorde,
        "CoordN": station.coordn,
        "Lat": station.lat,
        "Long": station.long,
        "ClimateRegion": station.climateregion,
    })


# POST: Add a new weather station
# To run this one needs to run this command:
# curl -X POST http://127.0.0.1:5000/weatherstation \
# -H "Content-Type: application/json" \
# -d '{ ... }'
# Inside the brackets one needs to input the new weather station with all its variables, in JSON format.
@app.route('/weatherstation', methods=['POST'])
def add_weatherstation():
    data = request.json
    if WeatherStation.query.get(data['weatherstationname']):
        return jsonify({"error": "WeatherStation already exists"}), 400
    new_station = WeatherStation(**data)
    db.session.add(new_station)
    db.session.commit()
    return jsonify({"message": "WeatherStation added successfully"}), 201


# PUT: Update an existing weather station
# To run this one needs to run this command:
# curl -X PUT http://127.0.0.1:5000/weatherstation/XXX \
# -H "Content-Type: application/json" \
# -d '{ ... }'
# Inside the brackets one needs to input the now updated weather station with all variables.
# Also the XXX needs to be replaced with the name of the weather station.
@app.route('/weatherstation/<string:weatherstation_name>', methods=['PUT'])
def update_weatherstation(weatherstation_name):
    station = WeatherStation.query.get(weatherstation_name)
    if not station:
        return jsonify({"error": "WeatherStation not found"}), 404
    data = request.json
    for key, value in data.items():
        if hasattr(station, key):
            setattr(station, key, value)
    db.session.commit()
    return jsonify({"message": "WeatherStation updated successfully"})


# DELETE: Delete a weather station
# To run this one needs to run this command:
# curl -X DELETE http://127.0.0.1:5000/weatherstation/XXX
# XXX would need to be replaced with the name of the to be deleted weather station.
@app.route('/weatherstation/<string:weatherstation_name>', methods=['DELETE'])
def delete_weatherstation(weatherstation_name):
    station = WeatherStation.query.get(weatherstation_name)
    if not station:
        return jsonify({"error": "WeatherStation not found"}), 404
    db.session.delete(station)
    db.session.commit()
    return jsonify({"message": "WeatherStation deleted successfully"})


# Run the App to be able to execute the statements on the table.
if __name__ == '__main__':
    app.run(debug=True)
