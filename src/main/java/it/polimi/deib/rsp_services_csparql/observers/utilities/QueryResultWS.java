package it.polimi.deib.rsp_services_csparql.observers.utilities;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

//@ServerEndpoint("/results/{queryName}")
@ServerEndpoint("/{queryName}")
public class QueryResultWS {
	
	private static Map<String, List<Session>> sessions = Collections.synchronizedMap(new HashMap<String,List<Session>>());
	
	@OnOpen
	public void onOpen(@PathParam("queryName") String queryName, Session session) {
		List<Session> sessionList = sessions.get(queryName);
		if (sessionList == null) {
			sessionList = new ArrayList<Session>();
		}
		sessionList.add(session);
		sessions.put(queryName, sessionList);
	}
	
	@OnClose
	public void onClose(@PathParam("queryName") String queryName, Session session) {
		List<Session> sessionList = sessions.get(queryName);
		sessionList.remove(session);
		if (sessionList.size() == 0) {
			sessions.remove(queryName);
		} else {
			sessions.put(queryName, sessionList);
		}
	}
		
	public static void sendToAllSession(String message, String queryName) throws IOException {
		if (sessions.containsKey(queryName)) {
			for (Session session: sessions.get(queryName)) {
				session.getBasicRemote().sendText(message);
			}
		}
	}
}
