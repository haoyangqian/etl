package edu.brown.cs.cs127.etl.model;

public class Airlines {
	private String airline_code;
	private String airline_name;
	
	public Airlines(String airline_code, String airline_name) {
		this.setAirline_code(airline_code);
		this.setAirline_name(airline_name);
	}
	
	public String getAirline_code() {
		return airline_code;
	}
	
	public void setAirline_code(String airline_code) {
		this.airline_code = airline_code;
	}
	
	public String getAirline_name() {
		return airline_name;
	}
	
	public void setAirline_name(String airline_name) {
		this.airline_name = airline_name;
	}
}
