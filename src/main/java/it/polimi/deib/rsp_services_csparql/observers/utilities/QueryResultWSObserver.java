package it.polimi.deib.rsp_services_csparql.observers.utilities;

import java.io.IOException;
import java.util.Observable;
import java.util.Observer;

import eu.larkc.csparql.common.RDFTable;

public class QueryResultWSObserver implements Observer {
	
	private String queryName = null;
	
	public QueryResultWSObserver(String queryName) {
		this.queryName = queryName;
	}

	@Override
	public void update(Observable observable, Object arg) {
		
		RDFTable table = (RDFTable) arg;

		try {
			QueryResultWS.sendToAllSession(table.getJsonSerialization(), queryName);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}		
}
