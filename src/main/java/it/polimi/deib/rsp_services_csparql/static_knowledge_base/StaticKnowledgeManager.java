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
package it.polimi.deib.rsp_services_csparql.static_knowledge_base;

import java.io.StringWriter;
import java.net.URI;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.engine.header.Header;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.larkc.csparql.common.RDFTable;
import it.polimi.deib.rsp_services_csparql.commons.Csparql_Engine;
import net.sf.saxon.TransformerFactoryImpl;

public class StaticKnowledgeManager extends ServerResource {

	private Logger logger = LoggerFactory.getLogger(StaticKnowledgeManager.class.getName());

	@Post
	public void execUpdateQuery(Representation rep){

		Gson gson = new Gson();
		Form f = new Form(rep);
		
		try {
			
			String origin = getRequest().getClientInfo().getAddress();
            Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
            if (responseHeaders == null) {
                responseHeaders = new Series<Header>(Header.class);
                getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
            }
            responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

			Csparql_Engine engine = (Csparql_Engine) getContext().getAttributes().get("csparqlengine");

			String action = f.getFirstValue("action");
			String iri;
			String serialization;
			String queryBody;
			
			switch (action) {
			case "put":
				iri = f.getFirstValue("iri");
				serialization = f.getFirstValue("serialization");
				engine.addStaticModel(iri, serialization);
				break;

			case "delete":
				iri = f.getFirstValue("iri");
				engine.removeStaticModel(iri);
				break;

			case "update":
				queryBody = f.getFirstValue("queryBody");
				engine.execUpdateQueryOverDatasource(queryBody);
				break;
				
			case "putKML":
				String fileKML = f.getFirstValue("fileKML");
				String fileXSLT = f.getFirstValue("fileXSLT");
				String featureType = f.getFirstValue("featureType");
							
				iri = fileKML;
				serialization = convertKML2RDF(fileKML, fileXSLT, featureType);
				engine.addStaticModel(iri, serialization);
				break;
				
			default:
				throw new Exception();
			}

			this.getResponse().setStatus(Status.SUCCESS_OK,"Update operation succeded.");
			this.getResponse().setEntity(gson.toJson("Update operation succeded."), MediaType.APPLICATION_JSON);

		} catch (Exception e) {
			logger.error("Problem while accessing internal static knowledge base or in the specified action");
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,"Problem during query Operation");
			this.getResponse().setEntity(gson.toJson("Problem during query Operation"), MediaType.APPLICATION_JSON);
		} finally {
			this.getResponse().commit();
			this.commit();	
			this.release();			
		}

	}
	
	private String convertKML2RDF(String fileKML, String fileXSLT, String featureType) {		
        System.setProperty("javax.xml.transform.TransformerFactory", "net.sf.saxon.TransformerFactoryImpl");        

        String output = ""; 
		
        try {
            StringWriter writer = new StringWriter();	
        	TransformerFactory tFactory = new TransformerFactoryImpl();
        	
            Transformer transformer = tFactory.newTransformer(new StreamSource(new URI(fileXSLT).toString()));  
            transformer.transform(new StreamSource(new URI(fileKML).toString()), new StreamResult(writer));  
            logger.debug("XSLT transformation completed successfully.");      
            output = writer.toString().replaceAll("GEOMETRYCOLLECTION", "MULTIPOLYGON");
            output = output.replaceAll(", POLYGON", ", ");
            output = output.replaceAll("\\(POLYGON\\(", "((");
            output = output.replaceAll("FEATURE_TYPE", featureType);
            logger.debug(output);
        } catch (Exception e) {  
        	e.printStackTrace();  
        }  
        
        return output;
	}

	@Get
	public void evaluateQuery(){
		try {

			Csparql_Engine engine = (Csparql_Engine) getContext().getAttributes().get("csparqlengine");

			String queryBody = getQueryValue("query");

			RDFTable result = engine.evaluateQueryOverDatasource(queryBody);
			String jsonSerialization = result.getJsonSerialization();

			this.getResponse().setStatus(Status.SUCCESS_OK,"Query evaluated.");
			this.getResponse().setEntity((jsonSerialization), MediaType.APPLICATION_JSON);

		} catch (Exception e) {
			logger.error("Problem while accessing internal static knowledge base: {}", e.getMessage());
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL,"Problem during query Operation");
			this.getResponse().setEntity(new Gson().toJson("Problem during query Operation"), MediaType.APPLICATION_JSON);
		} finally {
			this.getResponse().commit();
			this.commit();
			this.release();
		}

	}



}
