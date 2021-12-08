package model;

import java.math.BigDecimal;

public class Venda {
	private Long operacao;

	private Long cliente;

	private Integer quantidadeIngressos;

	private BigDecimal valorTotal;

	private String status;

	public Venda() {
	}

	public Venda(Long operacao, Long cliente, Integer quantidadeIngressos, BigDecimal valorTotal) {
		super();
		this.operacao = operacao;
		this.cliente = cliente;
		this.quantidadeIngressos = quantidadeIngressos;
		this.valorTotal = valorTotal;
	}

	@Override
	public String toString() {
		return "Venda [operaoção=" + operacao + ", cliente=" + cliente + ", quantidadeIngressos=" + quantidadeIngressos
				+ ", valorTotal=" + valorTotal + ", status=" + status + "]";
	}

	public Long getOperaoção() {
		return operacao;
	}

	public void setOperaoção(Long operaoção) {
		this.operacao = operaoção;
	}

	public Long getCliente() {
		return cliente;
	}

	public void setCliente(Long cliente) {
		this.cliente = cliente;
	}

	public Integer getQuantidadeIngressos() {
		return quantidadeIngressos;
	}

	public void setQuantidadeIngressos(Integer quantidadeIngressos) {
		this.quantidadeIngressos = quantidadeIngressos;
	}

	public BigDecimal getValorTotal() {
		return valorTotal;
	}

	public void setValorTotal(BigDecimal valorTotal) {
		this.valorTotal = valorTotal;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

}
