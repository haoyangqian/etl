package edu.brown.cs.cs127.etl.model;

public class Airports {
	private String airport_code;
	private String airport_name;
	private String city;
	private String state;
	
	public Airports(String airport_code, String airport_name, String city, String state) {
		this.setAirport_code(airport_code);
		this.setAirport_name(airport_name);
		this.setCity(city);
		this.setState(state);
	}
	public String getAirport_code() {
		return airport_code;
	}
	public void setAirport_code(String airport_code) {
		this.airport_code = airport_code;
	}
	public String getAirport_name() {
		return airport_name;
	}
	public void setAirport_name(String airport_name) {
		this.airport_name = airport_name;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
}
