-- This file contains all the necessary SQL statements to create all the tables needed for the project.
-- TODO: Add Index-Structure

CREATE TABLE WeatherStation (
	Canton VARCHAR(30),
	Station VARCHAR(30),
	WeatherStationName VARCHAR(30),
	WIGOSID VARCHAR(30),
	DataSince DATE,
	StationHeight FLOAT,
	CoordE FLOAT,
	CoordN FLOAT,
	Lat FLOAT,
	Long FLOAT,
	ClimateRegion VARCHAR(40),
	PRIMARY KEY (WeatherStationName)
);

CREATE TABLE Weather (
    WeatherStationName VARCHAR(30),
    Date DATE,
    GlobalRadiation FLOAT,
    TotalSnowDepth FLOAT,
    CloudCover FLOAT,
    Pressure FLOAT,
    Precipitation FLOAT,
    SunshineDuration FLOAT,
    AirTemperature_mean FLOAT,
    AirTemperature_min FLOAT,
    AirTemperature_max FLOAT,
    RelativeHumidity FLOAT,
    PRIMARY KEY (WeatherStationName, Date),
	FOREIGN KEY (WeatherStationName) REFERENCES WeatherStation(WeatherStationName)
);

CREATE TABLE TransportStation (
	BPUIC FLOAT,
	TStationName VARCHAR(30),
    BP_Abk VARCHAR(255),
	Canton VARCHAR(30),
    SLOID VARCHAR(255),
    BP_ID FLOAT,
	PRIMARY KEY (BPUIC)
);

CREATE TABLE TransportUndertaking (
    TU_CODE FLOAT PRIMARY KEY,
    TU_BEZEICHNUNG VARCHAR(255),
    TU_ABKUERZUNG VARCHAR(30)
);

CREATE TABLE TransportStationInfo (
	StationInfoID SERIAL PRIMARY KEY,
	FPID FLOAT,
	TU_Code FLOAT,
	Fartnummer FLOAT,
	BPUIC FLOAT,
	VM_Art VARCHAR(255),
	Fahrtage FLOAT,
	AB_Zeit_KB DATE,
	AN_Zeit_KB DATE,
	Richtung_Text_Aggregiert VARCHAR(255),
	END_BP_Bezeichnung VARCHAR(255),
	Linie VARCHAR(255),
	FOREIGN KEY (BPUIC) REFERENCES TransportStation(BPUIC),
    FOREIGN KEY (TU_CODE) REFERENCES TransportUndertaking(TU_CODE)
);


CREATE TABLE Map_To_Transport (
	BPUIC FLOAT,
	WeatherStationName VARCHAR(30),
	Canton VARCHAR(30),
	PRIMARY KEY (WeatherStationName, BPUIC),
	FOREIGN KEY (WeatherStationName) REFERENCES WeatherStation(WeatherStationName),
	FOREIGN KEY (BPUIC) REFERENCES TransportStation(BPUIC)
);

CREATE TABLE TransportEvent (
	TID SERIAL PRIMARY KEY,
	Date DATE,
	BPUIC FLOAT,
	ProduktID VARCHAR(30),
	ArrivalTime TIMESTAMP,
	DepartureTime TIMESTAMP,
	FaelltAus BOOLEAN DEFAULT FALSE,
	FOREIGN KEY (BPUIC) REFERENCES TransportStation(BPUIC)
);

CREATE TABLE TransportOperator (
    BetreiberID VARCHAR(30) PRIMARY KEY,
    BetreiberAbk VARCHAR(30),
    BetreiberName VARCHAR(100)
);

CREATE TABLE TransportJourney (
    Fahrt_Bezeichner VARCHAR(255) PRIMARY KEY,
    LinienID VARCHAR(255),
    LinienText VARCHAR(255),
    UmlaufID VARCHAR(255),
    VerkehrsmittelText VARCHAR(255)
);

CREATE TABLE TransportEventInfo (
	Fahrt_Bezeichner VARCHAR(255),
	BetreiberID VARCHAR(30),
	ArrivalTimePred TIMESTAMP,
	ArrivalPredStatus VARCHAR(30),
	DepartureTimePred TIMESTAMP,
	DeparturePredStatus VARCHAR(30),
	Zusatzfahrt_TF BOOLEAN DEFAULT FALSE,
	Durchfahrt_TF BOOLEAN DEFAULT FALSE,
	TID INTEGER,
	PRIMARY KEY (TID),
	FOREIGN KEY (TID) REFERENCES TransportEvent(TID),
    FOREIGN KEY (BetreiberID) REFERENCES TransportOperator(BetreiberID),
    FOREIGN KEY (Fahrt_Bezeichner) REFERENCES TransportJourney(Fahrt_Bezeichner)
);