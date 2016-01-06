/*
 * Copyright (c) 2016 Villu Ruusmann
 *
 * This file is part of JPMML-Piglet
 *
 * JPMML-Piglet is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Piglet is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Piglet.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.piglet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import com.google.common.base.Function;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.PMMLException;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PMMLFunc extends EvalFunc<Tuple> {

	private File pmmlFile = null;

	private Evaluator evaluator = null;

	private Function<FieldName, Integer> activeFieldMapper = null;


	public PMMLFunc(String pmmlPath){
		this.pmmlFile = new File(pmmlPath);

		if(!this.pmmlFile.exists()){
			throw new IllegalArgumentException("Local file " + pmmlPath + " not found");
		}
	}

	@Override
	public Tuple exec(Tuple input) throws PigException {

		if(this.evaluator == null){
			initialize(getInputSchema());
		}

		try {
			return EvaluatorUtil.exec(this.evaluator, this.activeFieldMapper, input);
		} catch(PigException pe){
			throw pe;
		} catch(PMMLException pe){
			return null;
		}
	}

	@Override
	public Schema outputSchema(Schema inputSchema){

		if(this.evaluator == null){

			try {
				initialize(inputSchema);
			} catch(PigException pe){
				throw new RuntimeException(pe);
			}
		}

		return EvaluatorUtil.outputSchema(this.evaluator, inputSchema);
	}

	@Override
	public List<String> getShipFiles(){
		return Collections.singletonList(this.pmmlFile.getAbsolutePath());
	}

	private void initialize(Schema inputSchema) throws PigException {
		File file;

		if(this.pmmlFile.exists()){
			file = this.pmmlFile;
		} else

		{
			file = new File("./" + this.pmmlFile.getName());
		}

		PMML pmml;

		try(InputStream is = new FileInputStream(file)){
			Source source = ImportFilter.apply(new InputSource(is));

			pmml = JAXBUtil.unmarshalPMML(source);
		} catch(JAXBException | SAXException | IOException e){
			throw new RuntimeException(e);
		}

		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();

		ModelEvaluator<?> evaluator = modelEvaluatorFactory.newModelManager(pmml);
		evaluator.verify();

		this.evaluator = evaluator;

		this.activeFieldMapper = EvaluatorUtil.inputSchemaMapper(evaluator, inputSchema);
	}
}