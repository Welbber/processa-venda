package main;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import model.Venda;
import serializer.VendaDeserializer;
import serializer.VendaSerializer;
import service.Consumer;
import service.Producer;
import utils.Log;

public class processaVenda {

	public final static String SERVER = "localhost:9092";

	public static void main(String[] args) {

		Consumer<String, Venda> consumer = new Consumer<String, Venda>(SERVER, true, StringDeserializer.class.getName(),
				VendaDeserializer.class.getName(), "grupo-processamento-venda", "topico_vendas");

		Producer<String, Venda> producer = new Producer<String, Venda>(SERVER, StringSerializer.class.getName(),
				VendaSerializer.class.getName());

		try {
			while (true) {
				ConsumerRecords<String, Venda> vendas = consumer.consumers(1);

				for (ConsumerRecord<String, Venda> venda : vendas) {
					processaVenda(venda.value());

					if (venda.value().getStatus().equals("ERRO")) {
						producer.send("topico-vendas-sucesso", venda.value());
					} else {
						producer.send("topico-vendas-erro", venda.value());
					}
				}
			}

		} catch (Exception e) {
			Log.err(processaVenda.class.getName(), e.getMessage());
		}

	}

	private static void processaVenda(Venda venda) {
		if (venda.getValorTotal().intValue() > 3500)
			venda.setStatus("ERRO");
		else
			venda.setStatus("SUCESSO");
	}
}
