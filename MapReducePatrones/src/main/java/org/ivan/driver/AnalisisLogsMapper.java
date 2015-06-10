package org.ivan.driver;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.ivan.writables.ClaveFechaProceso;
import org.ivan.writables.ValorProcesoCantidad;

public class AnalisisLogsMapper extends Mapper<LongWritable, Text, ClaveFechaProceso, ValorProcesoCantidad> {
	
	private static final String SEPARADOR_CLAVE = "#";
	private static final String PROCESO_A_EXCLUIR = "vmnet";
	private static final String FORMATO_FECHA = "dd/MM/yyyy";
	private static final String ANIO_FECHA = ", 2014";
	

	private HashMap <String,Long> hashMapNumProcesosDiaHora = new HashMap<String, Long>();
	

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] word = value.toString().split(" ");
		
		String mes = word[0];
		String dia = word[1];
		String horaMinutosSeg = word[2];
		String servicio = obtenerNombreProceso(word[4]);
		

		if(!servicio.equals("")){
			
			String claveNatural = obtenerClaveNatural(mes, dia, horaMinutosSeg);
			
			String claveCompuesta = claveNatural + SEPARADOR_CLAVE + servicio;
			
			// Se actualiza el hashmap
			if(hashMapNumProcesosDiaHora.containsKey(claveCompuesta)){
				hashMapNumProcesosDiaHora.put(claveCompuesta, hashMapNumProcesosDiaHora.get(claveCompuesta) + 1);
			}else{
				hashMapNumProcesosDiaHora.put(claveCompuesta,(long) 1);
			}
		
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		ClaveFechaProceso claveFechaProceso = new ClaveFechaProceso();
		ValorProcesoCantidad valorProcesoCantidad = new ValorProcesoCantidad();
		
		Iterator<Entry<String, Long>> it = hashMapNumProcesosDiaHora.entrySet().iterator();
		while (it.hasNext()) {
		    Entry<String, Long> e = it.next();

		    String claveCompuestaHasMap[] = e.getKey().toString().split(SEPARADOR_CLAVE);
		    claveFechaProceso.setFecha(new Text(claveCompuestaHasMap[0]));
		    claveFechaProceso.setProceso(new Text(claveCompuestaHasMap[1]));
		    
		    valorProcesoCantidad.setProceso(new Text(claveCompuestaHasMap[1]));
		    valorProcesoCantidad.setCantidad(new LongWritable(e.getValue()));
		    
		    context.write(claveFechaProceso, valorProcesoCantidad);
		}
		
	}
	
	/**
	 * 
	 * @param splitServicio
	 * @return funcion encargada de eliminar decada proceso el PID entre corchetes de cada proceso, en caso de tenerlo.
	 */
	private String obtenerNombreProceso(String splitServicio){
		
		// Se excluyen los procesos que tengan 'vmnet'
		if(splitServicio.indexOf(PROCESO_A_EXCLUIR) == -1){
			
			String[] a = splitServicio.split("\\[");
			
			if(a.length > 1){
				return a[0];
			}else{
				String[] b = splitServicio.split(":");
				return b[0];
			}
		}
		
		return "";
	}
	
	/**
	 * 
	 * @param mes
	 * @param dia
	 * @param horaMinutosSeg
	 * @return genera la clave neatural del JOB
	 */
	private String obtenerClaveNatural(String mes, String dia, String horaMinutosSeg){
		
		SimpleDateFormat sdf = new SimpleDateFormat(FORMATO_FECHA); 
		
		Date fechaOut = null;
		String fechaIn = mes + " " + dia + ANIO_FECHA; 
		DateFormat df = DateFormat.getDateInstance();
		
		try {
			fechaOut = df.parse(fechaIn);
		}catch(ParseException e) {
			System.out.println("Unable to parse " + fechaIn);
		}
		
		String fecha1 = sdf.format(fechaOut);
		String[] hora = horaMinutosSeg.split(":"); 

		
		return fecha1 + "-" + hora[0];
		
	}
}
