import java.sql.SQLException;

import core.Controller;
import core.Parser;


public class Main {

	/**
	 * Perturbation-based Method for Inference Control in Statistical Databases
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {

		String target=args[0];
		
		Controller controller=new Controller();
		controller.start(target);
		
		
	}

}
