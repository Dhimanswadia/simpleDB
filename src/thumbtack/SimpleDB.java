package thumbtack;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;

/**
 * author Jian Xiang
 * title: simple database from Thumbtack
 * Data Commands
 * Transaction Commands
 * Input / output formats
 */
public class SimpleDB {
	
	//hashmap to store name to all its values, we use string (not integer) to represent values
	HashMap<String, Stack<String>> nameToValue = null; 
	//hashmap to store the count of the keys that have the same value
	HashMap<String, Integer> valueCounter = null;
	//a stack of Transactions is used to store the keys 
	ArrayList<Transaction> transactions = null;
	
	public SimpleDB(){
		nameToValue = new HashMap<String, Stack<String>>();
		valueCounter = new HashMap<String, Integer>();
		transactions = new ArrayList<Transaction>();
	}
	
	/**
	 * Set the variable name to the value. 
	 * @param name: name of the variable
	 * @param value: In this implementation, we add the latest value to the stack
	 * corresponding to the name. 
	 */
	public void set(String name, String value){		          	            
	    // the variable has not been accessed before
	    if(!nameToValue.containsKey(name)){
	    	nameToValue.put(name, new Stack<String>());
	    }
	    // the variable has not been accessed in the current transaction or
	    if(transactions.size()>0){
	    	Transaction latest = transactions.get(transactions.size()-1);
			if(!latest.hasAccessed(name)){
				latest.accessedVariables.add(name);
			}else{
				/* if the current transaction has accessed the variable, pop the top of stack,
				 * so we do not add multiply values in one transaction 
				 */
				String oldVal = nameToValue.get(name).pop();
				updateValueCounter(oldVal, false);
			}
	    }
	    nameToValue.get(name).push(value);
	    updateValueCounter(value, true);
	}
	
	/**
	 * Get the value of the variable name, or NULL if that variable is not set.
	 * @param name the name of the variable
	 * @return the value of the variable, or NULL if that variable is not set; 
	 * in String format 
	 */
	public String get(String name){
		if(nameToValue.containsKey(name) && !nameToValue.get(name).peek().equals("UNSET")){
			return nameToValue.get(name).peek();
		}
		return "NULL";
	}
	
	/**
	 * Return the number of variables that are currently set to value. 
	 * If no variables equal that value, print 0.
	 * @param value: the value
	 * @return the number of variable that are currently set to the input value
	 */
	public int numEqualTo(String value){
		if(valueCounter.containsKey(value)){
			return valueCounter.get(value);
		}
		return 0;
	}
	
	/**
	 * Close all open transaction transactions, permanently applying the changes made in them. 
	 * @return false if no transaction is in progress.
	 */
	public boolean commit() {
		//if no transaction is in progress
		if (transactions.size()==0){
			return false;
		}	
		Set<String> allAccessedVariables = new HashSet<String>();
		//get all variables which have been accessed in this commit
		for(Transaction t : transactions){
			allAccessedVariables.addAll(t.accessedVariables);		
		}
		for(String variableName : allAccessedVariables){
			String topValue = nameToValue.get(variableName).pop();
			//if the value of this variable is "UNSET", delete the entry of the variable
			if(topValue.equals("UNSET")){
				nameToValue.remove(topValue);
			}
			// otherwise, set the value of a variable to the top element of the stack (latest value)
			nameToValue.get(variableName).clear();
			nameToValue.get(variableName).push(topValue);
		}
		return true;
	}
	
	/**
	 * Undo all of the commands issued in the most recent transaction block, 
	 * and discard the block.
	 * @return false if no previous block to roll back to.
	 */
	public boolean rollBack(){
		if (transactions.size() == 0 ) {
			return false;
		}
        // Iterate over all variables in the current transaction
		Transaction current = transactions.get(transactions.size()-1);
        for(String variableName : current.accessedVariables){
        	String val = nameToValue.get(variableName).pop();
        	updateValueCounter(val, false);
        }
		transactions.remove(transactions.size()-1);
		return true;
	}
	
	/**
	 * Open a new transaction block. Transaction transactions can be nested;
	 * BEGIN can be issued inside of an existing block.
	 */
	public void begin(){
		Transaction t = new Transaction();
		transactions.add(t);
	}
	
	/**
	 * a helper method for changing the count of variables that have the same value
	 * @param value 
	 * @param increasing increasing counter or decreasing counter
	 */
	private void updateValueCounter(String value, boolean increasing){
		if(value.equals("UNSET")){
			return;
		}
		if(increasing){
			if(!valueCounter.containsKey(value)){
				valueCounter.put(value, 0);
			}
			valueCounter.put(value, valueCounter.get(value)+1);
		}else{
			if(valueCounter.containsKey(value)){
				valueCounter.put(value, valueCounter.get(value)-1);
			}
			if(valueCounter.get(value)==0){
				valueCounter.remove(value);
			}
		}
	}
	
	public static void main(String[] args) {
		SimpleDB simpleDB = new SimpleDB();
		InputStream inputStream = System.in;
		if(args.length == 1){
			try {
				inputStream = new FileInputStream(args[0]);
				System.out.println("IN FILE MODE");
			} catch (FileNotFoundException e) {
				System.err.println("File doesn't exist, starting interactive mode");
				System.out.println("IN INTERACTIVE MODE");
			}
		}else{
			System.out.println("IN INTERACTIVE MODE");
		}
		Scanner scanner = new Scanner(inputStream);
		// space delimited
		scanner.useDelimiter("\\s+");
		// read the command
		String cmdString;
		while (scanner.hasNextLine()) {
			cmdString = scanner.nextLine();
			String[] tokens = cmdString.split("\\s+");
			String cmd = tokens[0];
			String name;
			String value;
			switch (cmd) {
				case "GET":
					name = tokens[1];
					System.out.println(simpleDB.get(name));
					break;
				case "SET":
					name = tokens[1];
					value = tokens[2];
					simpleDB.set(name, value);
					break;
				case "UNSET":
					name = tokens[1];
					simpleDB.set(name, null);
					break;
				case "NUMEQUALTO":
					value = tokens[1];
					System.out.println(simpleDB.numEqualTo(value));
					break;
				case "BEGIN":
					simpleDB.begin();
					break;
				case "ROLLBACK":
					if (!simpleDB.rollBack()){
						System.out.println("NO TRANSACTION");
					}
					break;
				case "COMMIT":
					if (!simpleDB.commit()){
						System.out.println("NO TRANSACTION");
					}
					break;					
				case "END":
					return;
				case "":
					break;
				default:
					System.out.println("Incorrect input command : " + cmd );
			}
		}
		scanner.close();
	}
}

/**
 * A transaction block conceptually includes all past uncommitted transactions accessible through traversing to the previous transaction block.
 */
class Transaction {	
	// transaction maintains a list of all variables that have been accessed in the transaction 
	Set<String> accessedVariables = new HashSet<String>();
	/**
	 * Check if the variable has been accessed in this transaction
	 * @param variable name
	 * @return true if the variable has been accessed in this transaction 
	 */
	public boolean hasAccessed(String name){
		return accessedVariables.contains(name);
	}
}
