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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.dmg.pmml.DataField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.OutputUtil;
import org.jpmml.evaluator.PMMLException;
import org.jpmml.evaluator.TypeAnalysisException;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PMMLFunc extends EvalFunc<Tuple> {

	private File pmmlFile = null;

	private Evaluator evaluator = null;

	private Map<FieldName, Integer> activeFieldIndices = null;


	public PMMLFunc(String pmmlPath){
		this.pmmlFile = new File(pmmlPath);

		if(!this.pmmlFile.exists()){
			throw new IllegalArgumentException("Local file " + pmmlPath + " not found");
		}
	}

	@Override
	public Tuple exec(Tuple input) throws PigException {
		Evaluator evaluator = ensureEvaluator();

		try {
			return evaluate(evaluator, input);
		} catch(PMMLException pe){
			return null;
		}
	}

	private Tuple evaluate(Evaluator evaluator, Tuple input) throws PigException {
		Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			Integer activeFieldIndex = this.activeFieldIndices.get(activeField);
			if(activeFieldIndex == null){
				throw new ExecException();
			}

			FieldValue activeValue = EvaluatorUtil.prepare(evaluator, activeField, input.get(activeFieldIndex));

			arguments.put(activeField, activeValue);
		}

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		List<Object> resultValues = new ArrayList<>();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			resultValues.add(EvaluatorUtil.decode(result.get(targetField)));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			resultValues.add(result.get(outputField));
		}

		return PMMLFunc.tupleFactory.newTuple(resultValues);
	}

	@Override
	public Schema outputSchema(Schema inputSchema){
		Evaluator evaluator = ensureEvaluator(inputSchema);

		List<Schema.FieldSchema> tupleFieldSchemas = new ArrayList<>();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			DataField dataField = evaluator.getDataField(targetField);

			org.dmg.pmml.DataType dataType = dataField.getDataType();

			tupleFieldSchemas.add(new Schema.FieldSchema(targetField.getValue(), DataTypeUtil.formatDataType(dataType)));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			OutputField output = evaluator.getOutputField(outputField);

			org.dmg.pmml.DataType dataType = output.getDataType();
			if(dataType == null){

				try {
					dataType = OutputUtil.getDataType(output, (ModelEvaluator<?>)evaluator);
				} catch(TypeAnalysisException tae){
					dataType = org.dmg.pmml.DataType.STRING;
				}
			}

			tupleFieldSchemas.add(new Schema.FieldSchema(outputField.getValue(), DataTypeUtil.formatDataType(dataType)));
		}

		FieldSchema tupleSchema;

		try {
			tupleSchema = new FieldSchema(PMMLFunc.class.getSimpleName(), new Schema(tupleFieldSchemas), DataType.TUPLE);
		} catch(FrontendException fe){
			throw new RuntimeException(fe);
		}

		return new Schema(tupleSchema);
	}

	@Override
	public List<String> getShipFiles(){
		return Collections.singletonList(this.pmmlFile.getAbsolutePath());
	}

	private Evaluator ensureEvaluator(){

		if(this.evaluator == null){

			try {
				initialize(getInputSchema());
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}

		return this.evaluator;
	}

	private Evaluator ensureEvaluator(Schema inputSchema){

		if(this.evaluator == null){

			try {
				initialize(inputSchema);
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}

		return this.evaluator;
	}

	private void initialize(Schema schema) throws JAXBException, SAXException, IOException {
		PMML pmml;

		try(InputStream is = new FileInputStream(resolvePMMLFile())){
			Source source = ImportFilter.apply(new InputSource(is));

			pmml = JAXBUtil.unmarshalPMML(source);
		}

		ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();

		ModelEvaluator<?> evaluator = modelEvaluatorFactory.newModelManager(pmml);

		Map<String, Integer> aliasIndices = new LinkedHashMap<>();

		List<Schema.FieldSchema> fieldSchemas = schema.getFields();
		for(int i = 0; i < fieldSchemas.size(); i++){
			Schema.FieldSchema fieldSchema = fieldSchemas.get(i);

			aliasIndices.put((fieldSchema.alias).toLowerCase(), Integer.valueOf(i));
		}

		Map<FieldName, Integer> activeFieldIndices = new LinkedHashMap<>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			DataField dataField = evaluator.getDataField(activeField);

			org.dmg.pmml.DataType dataType = dataField.getDataType();

			Integer aliasIndex = aliasIndices.get((activeField.getValue()).toLowerCase());
			if(aliasIndex == null){
				throw new IllegalArgumentException("Field " + activeField + " not defined");
			}

			FieldSchema fieldSchema = fieldSchemas.get(aliasIndex);

			org.dmg.pmml.DataType fieldDataType = DataTypeUtil.parseDataType(fieldSchema.type);

			if(!DataTypeUtil.isCompatible(dataType, fieldDataType)){
				throw new IllegalArgumentException("Field " + activeField + " does not support " + fieldDataType + " data. Must be " + dataType + " data");
			}

			activeFieldIndices.put(activeField, aliasIndex);
		}

		this.evaluator = evaluator;

		this.activeFieldIndices = activeFieldIndices;
	}

	private File resolvePMMLFile(){

		// Local file
		if(this.pmmlFile.exists()){
			return this.pmmlFile;
		}

		// Distributed cache file
		return new File(this.pmmlFile.getName());
	}

	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
}