package com.polestar.app;

import java.io.*;
import java.sql.*;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

class Data {
	public String name, alarmcolor, _id, paramString, datasourcecount, _alerticon, elementcount, uniqueid;
}

public class App 
{
    public static void main(String [] args) {
    	
    	String jdbcURL = "jdbc:mysql://localhost:3306/assets";
    	String dbusername = "dbusername";
    	String dbpassword = "dbpassword";
    	
    	String jsonFilePath = "./input.json";
    	//Create JSONParser Object
    	JSONParser jsonParser = new JSONParser();
    	
    	Connection connection = null;
    	Data data = new Data();
    	    	
    	try {
    		//Parse the content of JSON
    		Object jsonObject = (JSONObject) jsonParser.parse(new FileReader(jsonFilePath));
    		//Set DB Connections
    		connection = DriverManager.getConnection(jdbcURL, dbusername, dbpassword);
    		connection.setAutoCommit(false);
    		//Set SQLString values
    		String sqlStr = "INSERT INTO assets (name, alarmcolor, _id, parameters, datasourcecount, _alerticon, elementcount, uniqueid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    		PreparedStatement statement = connection.prepareStatement(sqlStr);
    		
    		
    		JSONArray jsonArray = (JSONArray) jsonObject;
    		
    		Iterator<String> al = jsonArray.iterator();
    		while(al.hasNext()) {
    			parseAssetObject( (Object) al.next() );
    		}
    		
    		statement.setString(1,data.name);
    		statement.setString(2,data.alarmcolor);
    		statement.setString(3,data._id);
    		statement.setString(4,data.paramString);
    		statement.setString(5,data.datasourcecount);
    		statement.setString(6,data._alerticon);
    		statement.setString(7,data.elementcount);
    		statement.setString(8,data.uniqueid);
    
    		
    		//execute sqlstr statement
    		statement.executeBatch();
    		
    		connection.commit();
    		connection.close();
    		   		
    	
    	} catch (ParseException err) {
    		err.printStackTrace();
    		
    	} catch (FileNotFoundException err) {
    		err.printStackTrace();
    		
    	} catch (IOException err) {
    		System.err.println(err);
    		
    	} catch (SQLException err) {
    		err.printStackTrace();
    		
    		try {
    			connection.rollback();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
    	}
        
    }
    
    
    private static void parseAssetObject(Object asset)
    {
    	JSONObject assetObject = (JSONObject) asset;
    	
    	Data data = new Data();
    	data.name = (String) assetObject.get("Name");
        data.alarmcolor = (String) assetObject.get("Alarmcolor");
        data._id = (String) assetObject.get("Id");
        JSONArray paramList = (JSONArray) assetObject.get("Parameters");
        data.datasourcecount = (String) assetObject.get("DatasourceCount");
        data._alerticon = (String) assetObject.get("_alertIcon");
        data.elementcount = (String) assetObject.get("ElementCount");
        data.uniqueid = (String) assetObject.get("UniqueID");

    	//Extract parameters array
    	Iterator<String> iteratorParams = paramList.iterator();
    	String paramString = null;
    	while(iteratorParams.hasNext()) {
    		paramString += iteratorParams.next() + "\n";
    	}
    	data.paramString = paramString;

    }

}
