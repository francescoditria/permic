package ui;

import java.util.Scanner;

public class CLI {

	public String showPrompt()
	{
		System.out.println("ready>");
		
		Scanner scan = new Scanner(System.in);
		String command = scan.nextLine();
		return command;
	}
	
	
	public void exit()
	{
		System.out.println("Bye");
	}

}
