package org.ivan.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.ivan.writables.ClaveFechaProceso;
import org.ivan.writables.ProcesoCantidad;
import org.ivan.writables.ValorProcesoCantidad;

public class AnalisisLogsReducer extends Reducer<ClaveFechaProceso, ValorProcesoCantidad, Text, Text> {

	
	private MultipleOutputs<Text, Text> multipleOutputs;
	
	private HashMap <String,Long> mapProcesosCantidad = new HashMap<String,Long>();
	
	
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	 
	@Override
	protected void reduce(ClaveFechaProceso key, Iterable<ValorProcesoCantidad> values, Context context) throws IOException, InterruptedException {
		
		Text outputValue = new Text();
		Text outputClave = new Text();
		
		StringBuilder sb = new StringBuilder("");
		
		List<ProcesoCantidad> listaProcesoCantidadDiaHora = new ArrayList<ProcesoCantidad>();
		ProcesoCantidad procesoCantidadDiaHora = null;
		
		
		for (ValorProcesoCantidad valorProcesoCantidad : values) {
		
			// procesos por dia y hora
			procesoCantidadDiaHora = new ProcesoCantidad();
			procesoCantidadDiaHora.setProceso(valorProcesoCantidad.getProceso().toString());
			procesoCantidadDiaHora.setCantidad(Long.valueOf(valorProcesoCantidad.getCantidad().toString()));
			
			listaProcesoCantidadDiaHora.add(procesoCantidadDiaHora);
			
			// counters
			if (mapProcesosCantidad.containsKey(valorProcesoCantidad.getProceso().toString())){
				mapProcesosCantidad.put(valorProcesoCantidad.getProceso().toString(), mapProcesosCantidad.get(valorProcesoCantidad.getProceso().toString()) + Long.valueOf(valorProcesoCantidad.getCantidad().toString()));
			}else{
				mapProcesosCantidad.put(valorProcesoCantidad.getProceso().toString(), Long.valueOf(valorProcesoCantidad.getCantidad().toString()));
			}
			
		}
		
		// Se ordena la lista de los procesos por cada hora de manera ascendente.
		Collections.sort(listaProcesoCantidadDiaHora, new Comparator<ProcesoCantidad>(){
			 
			@Override
			public int compare(ProcesoCantidad o1, ProcesoCantidad o2) {
				return o1.getCantidad().compareTo(o2.getCantidad());
			}
		}); 
		
		// 
		for (ProcesoCantidad procesoCantidad2 : listaProcesoCantidadDiaHora) {
			sb = sb.append(procesoCantidad2.getProceso() + ":" + procesoCantidad2.getCantidad() + ",");
		}
		
		// Se setean los valores del fichero de salida
		outputClave.set("[" + key.getFecha().toString() + "]");
		outputValue.set(sb.toString());
		
		multipleOutputs.write(outputClave, outputValue, generateFileName(key.getFecha()));
		
	}
	
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		Long contador = 0L;
		String proceso = "";
		
		// Procese de ordenacion de los procesos que se van a mostrar como Counters
		Set<Entry<String, Long>> set = mapProcesosCantidad.entrySet(); 
		List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(set);
		
		// Se ordena la lista
		Collections.sort(list, new Comparator<Object>(){
			public int compare(Object o1, Object o2) {
				return -((Comparable) ((Map.Entry) (o1)).getValue()).compareTo(((Map.Entry) (o2)).getValue());
			}
		}); 
		 
		for (Entry<String, Long> procesoCantidad3 : list) {
			
			proceso = procesoCantidad3.getKey();
			contador = procesoCantidad3.getValue();
			
			context.getCounter("Contador de procesos", proceso).increment(contador);
		}
		
		
		multipleOutputs.close();
	}
	
	/**
	 * 
	 * @param key
	 * @return nombre persolalizado de los ficheros de salida
	 */
	private String generateFileName(Text key){
		String[] dia = key.toString().split("/");
		return "dia-" + dia[0];		
	}
}
