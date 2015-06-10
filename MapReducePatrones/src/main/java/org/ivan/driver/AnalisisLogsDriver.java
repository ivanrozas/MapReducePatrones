package org.ivan.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ivan.writables.ClaveFechaProceso;
import org.ivan.writables.ValorProcesoCantidad;



public class AnalisisLogsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Invalid number of arguments\n\n" + "Usage: Analisis Logs <input_path> <output_path>\n\n");
			return -1;
		}
		
		String input = args[0];
		String output = args[1];
		
		
		//borrado de ficheros de salida
		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);
		
		
		Job job = Job.getInstance(getConf(), "Analisis Logs");
		job.setJarByClass(AnalisisLogsDriver.class);
		job.setJobName("Analisis Logs");

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setInputFormatClass(TextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		//Establecemos el número de tareas Reduce
		job.setNumReduceTasks(2);
		  
		job.setMapOutputKeyClass(ClaveFechaProceso.class);
		job.setMapOutputValueClass(ValorProcesoCantidad.class);
		
		job.setPartitionerClass(ProcesoPartitioner.class);
		job.setSortComparatorClass(FechaProcesoComparator.class);
		
		job.setMapperClass(AnalisisLogsMapper.class);
		job.setReducerClass(AnalisisLogsReducer.class);
		
		
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new AnalisisLogsDriver(), args);
	}

}
