-- public.cliente definition

-- Drop table

-- DROP TABLE public.cliente;

CREATE TABLE public.cliente (
	nom_tipo_docm varchar NOT NULL,
	nom_ende_emai_clie varchar NULL,
	nom_clie varchar NULL,
	cod_rfrc_clie varchar NULL,
	cod_tipo_pess varchar NULL,
	cliente varchar NULL,
	CONSTRAINT cliente_pk PRIMARY KEY (nom_tipo_docm)
);

-- public.endereco definition

-- Drop table

-- DROP TABLE public.endereco;

CREATE TABLE public.endereco (
	nom_cida_ende_entg varchar NULL,
	nom_cmpl_ende_entg varchar NULL,
	cod_pais_resd varchar NULL,
	nom_bair_ende_entg varchar NULL,
	num_logr_ende_entg varchar NULL,
	nom_pess varchar NULL,
	sig_uf varchar NULL,
	nom_estd varchar NULL,
	nom_logr_ende_entg varchar NULL,
	cod_cep varchar NOT NULL,
	cliente varchar NOT NULL,
	CONSTRAINT endereco_pk PRIMARY KEY (cod_cep, cliente)
);


-- public.endereco foreign keys

ALTER TABLE public.endereco ADD CONSTRAINT endereco_fk FOREIGN KEY (cliente) REFERENCES public.cliente(nom_tipo_docm);

-- public.items definition

-- Drop table

-- DROP TABLE public.items;

CREATE TABLE public.items (
	des_moda_entg_item varchar NULL,
	qtd_dia_prz_entg varchar NULL,
	vlr_totl_fret varchar NULL,
	nom_oped_empr varchar NULL,
	nom_item varchar NULL,
	cod_ofrt_ineo_requ varchar NULL,
	vlr_oril float8 NULL,
	vlr_prod float8 NULL,
	vlr_prod_ofrt_desc float8 NULL,
	nom_prod_orig varchar NULL,
	txt_plae_otmz_url_prod varchar NULL,
	qtd_item_pedi varchar NULL,
	idt_venr varchar NULL,
	nom_venr varchar NULL,
	idt_vrne_venr varchar NULL,
	vlr_fret float8 NULL,
	vlr_desc_envo float8 NULL,
	cod_vrne_prod varchar NULL,
	txt_plae_otmz_url_item varchar NULL,
	nom_url_img_oril varchar NULL,
	vlr_oril_prod_unit_vrne float8 NULL,
	vlr_prec_unit_brut float8 NULL,
	pedido varchar NULL,
	id_item serial4 NOT NULL,
	CONSTRAINT items_pk PRIMARY KEY (id_item)
);


-- public.items foreign keys

ALTER TABLE public.items ADD CONSTRAINT items_fk FOREIGN KEY (pedido) REFERENCES public.pedidos(txt_detl_idt_pedi_pgto);

-- public.pedidos definition

-- Drop table

-- DROP TABLE public.pedidos;

CREATE TABLE public.pedidos (
	cod_aces_tokn varchar NULL,
	cod_idef_clie varchar NULL,
	dat_atui varchar NULL,
	dat_hor_tran varchar NULL,
	idt_loja varchar NULL,
	num_ulti_vers_sist varchar NULL,
	stat_pedi varchar NULL,
	stat_pgto varchar NULL,
	txt_detl_idt_pedi_pgto varchar NOT NULL,
	cliente varchar NULL,
	CONSTRAINT pedidos_pk PRIMARY KEY (txt_detl_idt_pedi_pgto)
);


-- public.pedidos foreign keys

ALTER TABLE public.pedidos ADD CONSTRAINT pedidos_fk FOREIGN KEY (cliente) REFERENCES public.cliente(nom_tipo_docm);

-- public.resumo definition

-- Drop table

-- DROP TABLE public.resumo;

CREATE TABLE public.resumo (
	num_item varchar NULL,
	qtd_maxi_temp_envo varchar NULL,
	vlr_desc_conc_pedi float8 NULL,
	vlr_totl_desc float8 NULL,
	vlr_cust_totl_envo float8 NULL,
	vlr_desc_totl_envo float8 NULL,
	pedido varchar NULL,
	id_resumo serial4 NOT NULL,
	CONSTRAINT resumo_pk PRIMARY KEY (id_resumo)
);


-- public.resumo foreign keys

ALTER TABLE public.resumo ADD CONSTRAINT resumo_fk FOREIGN KEY (pedido) REFERENCES public.pedidos(txt_detl_idt_pedi_pgto);

-- public.tel_clie definition

-- Drop table

-- DROP TABLE public.tel_clie;

CREATE TABLE public.tel_clie (
	cod_tel_clie varchar NULL,
	cod_area_tel varchar NULL,
	cod_pais_tel varchar NULL,
	num_tel_clie varchar NOT NULL,
	cliente varchar NOT NULL,
	CONSTRAINT tel_clie_pk PRIMARY KEY (cliente, num_tel_clie)
);


-- public.tel_clie foreign keys

ALTER TABLE public.tel_clie ADD CONSTRAINT tel_clie_fk FOREIGN KEY (cliente) REFERENCES public.cliente(nom_tipo_docm);