package core;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;

public class Engine {

	Connection connection;	
	String database;
	
	static Map<String, String> tableMap;
	static Map<String, Block> columnMap;

	public void init(String username,String password, String host,String port,String database)
	{
		String jdbc="jdbc:mysql://"+host+":"+port+"/"+database;

        try 
        {
			Class.forName("com.mysql.jdbc.Driver");
	        connection = (Connection) DriverManager.getConnection(jdbc, username, password);
	        this.database=database;
	        System.out.println("Connected to "+database);
	        
	        tableMap=new HashMap<String, String>();
	        columnMap=new HashMap<String, Block>();
	    
	        this.exractProfile();
		    
		
        } catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			System.out.println("Connection error");
		}
       
	}
	
	/////////////////////////////////////QUERY//////////////////////////////////
	
	
	public void exec(String function,String columnName,String tableName,String where, String query) throws SQLException
	{
		//System.out.println(function);
		if(!function.equals("sum") && !function.equals("avg") && !function.equals("count"))
		{
			System.out.println("Invalid function");
			return;
		}

		
		String tableRows=tableMap.get(tableName);
		if (tableRows == null)
		{
			System.out.println("Table not found");
			return;
		}
		
		Block block=this.columnMap.get(tableName+"."+columnName);
		String stddev=block.stddev;		
		Float st=Float.parseFloat(stddev);
		
		String cwhere="";
		if(!where.isEmpty()) cwhere= " where "+where;

		long tr = Long.parseLong(tableRows);//numero di righe della tabella
		double g=block.factor;//this.setFactor(st); //fattore
		
		long n=(long) (tr/g);	//righe da estrarre
		System.out.println("Rows\t"+tr+"\tN\t"+n+"\tFactor\t"+g);
		
		String query2="select "+function+"("+columnName+") from (select * from "+tableName+cwhere+" LIMIT 0,"+n+") as t";
		//System.out.println(query2);

		Statement statement=connection.createStatement();
		ResultSet rs;
		Double result;
		double finale;		
		double start_time;
		double final_time;
		double difference;
		
		start_time = System.currentTimeMillis();
		rs = statement.executeQuery(query2);
		rs.next();
		result=rs.getDouble(1);
		//System.out.println("Result\t"+result);

		//double fattore=tr/n;
		//System.out.println("Factor\t"+fattore);
		finale=result;
		if(function.equals("sum") || function.equals("count")) 
        {
			//finale=fattore*result;
			finale=g*result;
		}

		final_time = System.currentTimeMillis();
		difference = (final_time - start_time)/1000;
		System.out.println("\t-->"+Math.round(finale)+"\t"+difference+" secs");

		//System.out.println("STD DEV\t"+stddev);
		//System.out.println("["+Math.round(finale-(st))+","+Math.round(finale+(st))+"]\twith p = 67%");
		//System.out.println("["+Math.round(finale-(2*st))+","+Math.round(finale+(2*st))+"]\twith p = 95%");
		//System.out.println("["+Math.round(finale-(3*st))+","+Math.round(finale+(3*st))+"]\twith p = 97%");
		

		/*//////////////////exact answer
		start_time = System.currentTimeMillis();
		rs = statement.executeQuery(query);
		rs.next();
		result=rs.getDouble(1);
		final_time = System.currentTimeMillis();
		difference = (final_time - start_time)/1000;
		System.out.println("exact\t-->"+result+"\t"+difference+" secs");
		*/
		
		
	}
	
	private double setFactor(float st)
	{
		double g;
		if(st>50)
			g=1.1;
		else if(st>40)
			g=1.2;
		else if(st>30)
			g=1.3;
		else if(st>20)
			g=1.4;
		else if(st>10)
			g=1.5;
		else if(st>5)
			g=2;
		else if(st>2)
			g=5;
		else
			g=10;

		return g;
	}
	
	//////////////////////////////////////////////////////METADATA///////////////////////
	
	private void exractProfile() throws SQLException
	{
		
		this.getTables();
	}
	
	private void getTables() throws SQLException
	{
		//System.out.println("Extracting tables");
        ResultSet rs;
        ResultSet rs2;
		Statement statement=connection.createStatement();
        String query="select TABLE_NAME, TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA='"+database+"' and TABLE_TYPE='BASE TABLE'";
        rs = statement.executeQuery(query);
        while (rs.next()) {
        	
        	String tableName=rs.getString("TABLE_NAME");
        	//String tableRows=rs.getString("TABLE_ROWS");
        	Statement statement2=connection.createStatement();
            String query2="select count(*) as N from "+database+"."+tableName;
            rs2 = statement2.executeQuery(query2);
            rs2.next();
            String tableRows=rs2.getString("N");
        	
        	//System.out.println(tableName+ "\t" +tableRows);
        	tableMap.put(tableName, tableRows);
        	this.getColumns(tableName);
        }

	}
	
	private void getColumns(String tableName) throws SQLException
	{
		
		//System.out.println("Extacting columns in "+tableName);
		ResultSet ds;

		ds = connection.getMetaData().getColumns(database, null,tableName, null);
	    while (ds.next()) {
         	String columnName=ds.getString("COLUMN_NAME");
         	String columnType=ds.getString("TYPE_NAME");
            //System.out.println(columnName);
         	if(this.isSafeType(columnType) && !this.isPK(tableName, columnName) && !this.isFK(tableName, columnName))
         	{
         		this.getSummary(tableName, columnName);
         	}
            
	    }
	}

	
	private void getSummary(String tableName, String columnName)  throws SQLException
	{
		//System.out.println("Computing summaries of "+columnName +" in "+tableName);
		String query="select min("+columnName+") as min,max("+columnName+") as max,round(avg("+columnName+"),2) as avg,round(stddev("+columnName+"),2) as stddev from "+tableName;
		String y[] = new String[4];
        Statement statement=connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        rs.next();
        //System.out.println(rs.getString("n"));
        y[0]=rs.getString("min");
        y[1]=rs.getString("max");
        y[2]=rs.getString("avg");
        y[3]=rs.getString("stddev");

        //System.out.println(y[0]+"\t"+y[1]+"\t"+y[2]+"\t"+y[3]);
		
        Block block=new Block();
        block.min=y[0];
        block.max=y[1];
        block.avg=y[2];
        block.stddev=y[3];
        
        
        double cf=Double.parseDouble(y[3])/Double.parseDouble(y[2]);
        double cf2;
        double min=0.1;
        double max=1;
        double min2=10;
        double max2=90;
        
        if(cf<=min) cf2=min2;
        else if(cf>=max) cf2=max2;
        else cf2=(cf-min)/(max-min)*(max2-min2)+min2;
        
        block.factor=100/cf2;
        System.out.println(tableName+"."+columnName+"\tAVG "+y[2]+"\tSTD "+y[3]+"\tCF "+cf+"\tCF2 "+cf2+"\tFact "+block.factor);
        columnMap.put(tableName+"."+columnName, block);
        
        
	}
	
	
	private boolean isSafeType(String type)
	{
		type=type.toUpperCase();
		int i;
		String[] safe={"INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT","DECIMAL", "NUMERIC","FLOAT","DOUBLE"};
		
		int n=safe.length;
		
		for(i=0;i<n;i++)
		{
			if(type.equals(safe[i]))
					return true;
		}
		
		return false;
	}

	
	private boolean isPK(String tableName,String columnName) throws SQLException
	{
		DatabaseMetaData meta;
	 	meta = (DatabaseMetaData) connection.getMetaData();
	 	ResultSet rs=meta.getPrimaryKeys(null, null, tableName);
	 	while(rs.next())
	 	{
	 		String pk= rs.getString(4);
	 		//System.out.println(pk);
	 		if(columnName.equals(pk))
	 		{
	 			return true;
	 		}
	 	}

		return false;
	}

	
	private boolean isFK(String tableName,String columnName) throws SQLException
	{
		DatabaseMetaData meta;
	 	meta = (DatabaseMetaData) connection.getMetaData();
	 	ResultSet rs=meta.getImportedKeys(null, null, tableName);
	 	while(rs.next())
	 	{
	 		String fk= rs.getString(8);
	 		//System.out.println(fk);
	 		if(columnName.equals(fk))
	 		{
	 			return true;
	 		}
	 	}

		return false;
	}

}
