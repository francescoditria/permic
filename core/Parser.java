package core;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

	Engine engine=new Engine();
	
	public void getParameters(String target)
	{
		
		String username = "";
		String password="";
		String host = "";
		String port = "";
		String database = "";
		
		Pattern pattern;
		Matcher matcher;
		String operation="(.+)\\.(.+)@(.+):(.+)/(.+)";
		pattern=Pattern.compile(operation);
		matcher=pattern.matcher(target);
		while(matcher.find()){
			username=matcher.group(1);
			password=matcher.group(2);
			host=matcher.group(3);
			port=matcher.group(4);
			database=matcher.group(5);					
		}

		engine.init(username, password, host, port, database);
		
	}
	
	
	public boolean parse(String query) throws SQLException
	{
		
		query=query.toLowerCase();

		Pattern pattern;
		Matcher matcher;
		String operation;
		String function="";
		String columnName="";
		String tableName="";
		String where="";				

		
		operation="bye";
		pattern=Pattern.compile(operation);
		matcher=pattern.matcher(query);
		while(matcher.find()){
			return true;
		}

		operation="select (.+)\\((.+)\\) from (.+) where (.+)";
		pattern=Pattern.compile(operation);
		matcher=pattern.matcher(query);
		while(matcher.find()){
			function=matcher.group(1);
			columnName=matcher.group(2);
			tableName=matcher.group(3);
			where=matcher.group(4);				
			engine.exec(function,columnName,tableName,where,query);
			return false;
		}

		
		operation="select (.+)\\((.+)\\) from (.+)";
		pattern=Pattern.compile(operation);
		matcher=pattern.matcher(query);
		while(matcher.find()){
			function=matcher.group(1);
			columnName=matcher.group(2);
			tableName=matcher.group(3);
			engine.exec(function, columnName, tableName, where, query);
			return false;
		}
		
		return false;


		
	}
	
	
}