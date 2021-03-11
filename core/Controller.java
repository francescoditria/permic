package core;

import java.sql.SQLException;

import ui.CLI;

public class Controller {
	
	Parser parser=new Parser();
	String version="1.1";
	
	public void start(String target) throws SQLException
	{
		
		parser.getParameters(target);
		CLI cli=new CLI();
		String query;
		boolean stop;

		cli.showIntro(version);
		do 
		{
			query=cli.showPrompt();
			stop=parser.parse(query);
		}
		while(stop==false);
		cli.exit();
		System.exit(0);
		
		
	}

	
	
}
