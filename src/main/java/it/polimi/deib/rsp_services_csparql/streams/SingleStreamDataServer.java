/*******************************************************************************
 * Copyright 2014 DEIB - Politecnico di Milano
 *
 * Marco Balduini (marco.balduini@polimi.it)
 * Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This work was partially supported by the European project LarKC (FP7-215535) and by the European project MODAClouds (FP7-318484)
 ******************************************************************************/
package it.polimi.deib.rsp_services_csparql.streams;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import org.restlet.data.ClientInfo;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.engine.header.Header;
import org.restlet.representation.Representation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Options;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamreasoning.rsp_services.commons.Rsp_services_Component_Status;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
import it.polimi.deib.rsp_services_csparql.commons.Csparql_Engine;
import it.polimi.deib.rsp_services_csparql.commons.Csparql_RDF_Stream;
import it.polimi.deib.rsp_services_csparql.commons.Utilities;
import it.polimi.deib.rsp_services_csparql.streams.utilities.CsparqlStreamDescriptionForGet;

public class SingleStreamDataServer extends ServerResource {

	private static Hashtable<String, Csparql_RDF_Stream> csparqlStreamTable;
	private Csparql_Engine engine;
	private Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	private Logger logger = LoggerFactory.getLogger(SingleStreamDataServer.class.getName());

	@SuppressWarnings("unchecked")
	@Options
	public void optionsRequestHandler(){
		ClientInfo c = getRequest().getClientInfo();
		String origin = c.getAddress();
		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
			responseHeaders = new Series<Header>(Header.class);
			getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));
		responseHeaders.add(new Header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE"));

	}

    @SuppressWarnings({ "unchecked" })
    @Get
    public void getStreamsInformations(){

        try{

            String origin = getRequest().getClientInfo().getAddress();
            Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
            if (responseHeaders == null) {
                responseHeaders = new Series<Header>(Header.class);
                getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
            }
            responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

            csparqlStreamTable = (Hashtable<String, Csparql_RDF_Stream>) getContext().getAttributes().get("csaprqlinputStreamTable");
            ArrayList<CsparqlStreamDescriptionForGet> streamDescriptionList = new ArrayList<CsparqlStreamDescriptionForGet>();

            Set<String> keySet = csparqlStreamTable.keySet();
            Csparql_RDF_Stream registeredCsparqlStream;
            for(String key : keySet){
                registeredCsparqlStream = csparqlStreamTable.get(key);
                streamDescriptionList.add(new CsparqlStreamDescriptionForGet(registeredCsparqlStream.getStream().getIRI(), registeredCsparqlStream.getStatus()));
            }

            this.getResponse().setStatus(Status.SUCCESS_OK,"Information about streams succesfully extracted");
            this.getResponse().setEntity(gson.toJson(streamDescriptionList), MediaType.APPLICATION_JSON);

        } catch(Exception e){
            logger.error("Error while getting multiple streams informations", e);
            this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,"Generic Error");
            this.getResponse().setEntity(gson.toJson("Generic Error"), MediaType.APPLICATION_JSON);
        } finally{
            this.getResponse().commit();
            this.commit();
            this.release();
        }

    }

	@SuppressWarnings({ "unchecked" })
	@Put
	public void registerStream(){

		try{

			String origin = getRequest().getClientInfo().getAddress();
			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series<Header>(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));
			
			csparqlStreamTable = (Hashtable<String, Csparql_RDF_Stream>) getContext().getAttributes().get("csaprqlinputStreamTable");
			engine = (Csparql_Engine) getContext().getAttributes().get("csparqlengine");

			String inputStreamName = URLDecoder.decode((String) this.getRequest().getAttributes().get("streamname"), "UTF-8");

			if(!csparqlStreamTable.containsKey(inputStreamName)){
				RdfStream stream = new RdfStream(inputStreamName);
				Csparql_RDF_Stream csparqlStream = new Csparql_RDF_Stream(stream, Rsp_services_Component_Status.RUNNING);
				csparqlStreamTable.put(inputStreamName, csparqlStream);
				engine.registerStream(stream);
				getContext().getAttributes().put("csaprqlinputStreamTable", csparqlStreamTable);
				getContext().getAttributes().put("csparqlengine", engine);
				this.getResponse().setStatus(Status.SUCCESS_OK,"Stream " + inputStreamName + " succesfully registered");
				this.getResponse().setEntity(gson.toJson("Stream " + inputStreamName + " succesfully registered"), MediaType.APPLICATION_JSON);
			} else {
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,inputStreamName + " already exists");
				this.getResponse().setEntity(gson.toJson(inputStreamName + " already exists"), MediaType.APPLICATION_JSON);
			}
		} catch(Exception e){
			logger.error(e.getMessage(), e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, e.getMessage());
			this.getResponse().setEntity(gson.toJson(e.getMessage()), MediaType.APPLICATION_JSON);
		} finally{
			this.getResponse().commit();
			this.commit();	
			this.release();
		}

	}

	@SuppressWarnings({ "unchecked" })
	@Delete
	public void unregisterStream(){
		try{

			String origin = getRequest().getClientInfo().getAddress();
			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series<Header>(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));
			
			csparqlStreamTable = (Hashtable<String, Csparql_RDF_Stream>) getContext().getAttributes().get("csaprqlinputStreamTable");
			engine = (Csparql_Engine) getContext().getAttributes().get("csparqlengine");

			String inputStreamName = URLDecoder.decode((String) this.getRequest().getAttributes().get("streamname"), "UTF-8");

			if(csparqlStreamTable.containsKey(inputStreamName)){
				RdfStream stream = csparqlStreamTable.get(inputStreamName).getStream();
				engine.unregisterStream(stream.getIRI());
				csparqlStreamTable.remove(inputStreamName);
				getContext().getAttributes().put("csaprqlinputStreamTable", csparqlStreamTable);
				getContext().getAttributes().put("csparqlengine", engine);
				this.getResponse().setStatus(Status.SUCCESS_OK,"Stream " + inputStreamName + " succesfully unregistered");
				this.getResponse().setEntity(gson.toJson("Stream " + inputStreamName + " succesfully unregistered"), MediaType.APPLICATION_JSON);
			} else {
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,inputStreamName + " does not exist");
				this.getResponse().setEntity(gson.toJson(inputStreamName + " does not exist"), MediaType.APPLICATION_JSON);
			}

		} catch(Exception e){
			logger.error("Error while unregistering stream", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,Utilities.getStackTrace(e));
			this.getResponse().setEntity(gson.toJson(Utilities.getStackTrace(e)), MediaType.APPLICATION_JSON);
		} finally{
			this.getResponse().commit();
			this.commit();	
			this.release();
		}
	}

	@SuppressWarnings({ "unchecked" })
	@Post
	public void feedStream(Representation rep){

		try{

			String origin = getRequest().getClientInfo().getAddress();
			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series<Header>(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));
			
			csparqlStreamTable = (Hashtable<String, Csparql_RDF_Stream>) getContext().getAttributes().get("csaprqlinputStreamTable");
			engine = (Csparql_Engine) getContext().getAttributes().get("csparqlengine");

			String inputStreamName = URLDecoder.decode((String) this.getRequest().getAttributes().get("streamname"), "UTF-8");

			if(csparqlStreamTable.containsKey(inputStreamName)){
				Csparql_RDF_Stream streamRepresentation = csparqlStreamTable.get(inputStreamName);

				String jsonSerialization = rep.getText();

				Model model = deserializeAsJsonSerialization(jsonSerialization, null);
				
				long ts = System.currentTimeMillis();

				StmtIterator it = model.listStatements();
				while(it.hasNext()){
					Statement st = it.next();
					streamRepresentation.feed_RDF_stream(new RdfQuadruple(st.getSubject().toString(), st.getPredicate().toString(), st.getObject().toString(), ts));
				}

				this.getResponse().setStatus(Status.SUCCESS_OK,"Stream " + inputStreamName + " succesfully feeded");
				this.getResponse().setEntity(gson.toJson("Stream " + inputStreamName + " succesfully feeded"), MediaType.APPLICATION_JSON);

			} else {
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,"Specified stream does not exists");
				this.getResponse().setEntity(gson.toJson("Specified stream does not exists"), MediaType.APPLICATION_JSON);
			}

		} catch(Exception e){
			logger.error("Error while changing status of a stream", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, Utilities.getStackTrace(e));
			this.getResponse().setEntity(gson.toJson(Utilities.getStackTrace(e)), MediaType.APPLICATION_JSON);
		} finally{
			this.getResponse().commit();
			this.commit();	
			this.release();
		}
    }
    
	public static Model deserializeAsJsonSerialization(String asJsonSerialization, JsonLdOptions options) {

		Model model = ModelFactory.createDefaultModel();

		try {
			
			RDFDataset dataset = null;

			try {				
				Object jsonObject = JsonUtils.fromString(asJsonSerialization);

				if (options != null) {
					dataset = (RDFDataset) JsonLdProcessor.toRDF(jsonObject, options);
				} else {
					dataset = (RDFDataset) JsonLdProcessor.toRDF(jsonObject);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			for (String graphName : dataset.graphNames()) {

				for (RDFDataset.Quad q : dataset.getQuads(graphName)) {
					
					Resource subject = null;					
					if (q.getSubject().isBlankNode()) {
						subject = new ResourceImpl(new AnonId(q.getSubject().getValue()));
					} else {
						subject = new ResourceImpl(q.getSubject().getValue());
					}

					Property predicate = new PropertyImpl(q.getPredicate().getValue());

					
					if (!q.getObject().isLiteral()) {
						
						Resource object = null;
						if (q.getObject().isBlankNode()) {
							object = new ResourceImpl(new AnonId(q.getObject().getValue()));
						} else {
							object = new ResourceImpl(q.getObject().getValue());
						}
						
						model.add(subject, predicate, object);
						
					} else {
						
						RDFDatatype type = NodeFactory.getType(q.getObject().getDatatype());
						Literal typedLiteral = ResourceFactory.createTypedLiteral(q.getObject().getValue(), type);
						
						model.add(subject, predicate, typedLiteral);
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return model;
	}
}
