import plotly.graph_objects as go
from dash import dcc, Dash, html
from dash.dependencies import Input, Output
import pandas as pd
from PIL import Image
import cx_Oracle
from datetime import datetime

dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
cur = connection.cursor()
cur.execute(
    r"SELECT ROUND (SUM (MOV.QTDE),2) AS MP_CONSUM, EXTRACT(MONTH FROM MOV.DT) AS MES "
    r" FROM FOCCO3I.TMOV_ESTQ MOV "
    r",FOCCO3I.TITENS_ESTOQUE EST "
    r",FOCCO3I.TGRP_CLAS_ITE CLA "
    r",FOCCO3I.TITENS TIT "
    r",FOCCO3I.TTP_MOV_ESTQ TTP "
    r"WHERE MOV.ITESTQ_ID = EST.ID "
    r"AND EST.GRP_CLAS_ID = CLA.ID "
    r"AND EST.COD_ITEM = TIT.COD_ITEM "
    r"AND MOV.TMVES_ID = TTP.ID "
    r"AND TTP.SIGLA = 'REP' "
    r"AND MOV.ALMOX_ID_ORIG in (590,591) "
    r"AND MOV.DT BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY EXTRACT (month FROM MOV.DT) "
    r"ORDER BY MES "
)

mpconsumida = cur.fetchall()
mpconsu = pd.DataFrame(mpconsumida, columns=['MP_CONSUM', 'MES'])

connection.commit()
cur.execute(
    r"SELECT ROUND (SUM(ITPDV.QTDE*ITPDV.PESO_LIQ),2) AS CAR_FECH, EXTRACT(month FROM CAR.DATA_FIM) as mes "
    r"FROM FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR "
    r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS_IT CARIT ON CARIT.SR_CARREG_ID = CAR.ID "
    r"INNER JOIN FOCCO3I.TITENS_PDV ITPDV                     ON ITPDV.ID = CARIT.ITPDV_ID "
    r"INNER JOIN FOCCO3I.TITENS_COMERCIAL CO                  ON CO.ID = ITPDV.ITCM_ID "
    r"INNER JOIN FOCCO3I.TITENS_ENGENHARIA ENG                ON ENG.ITEMPR_ID = CO.ITEMPR_ID "
    r"WHERE ENG.TP_ITEM = 'F' "
    r"AND CAR.SITUACAO = 'F' "
    r"AND CAR.CARREGAMENTO > 264400 "
    r"AND CAR.DATA_FIM BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY EXTRACT(month FROM CAR.DATA_FIM) "
    r"ORDER BY 2 "
)
pesofabfech = cur.fetchall()
pesofabfech = pd.DataFrame(pesofabfech, columns=['CAR_FECH', 'MES'])

connection.commit()
cur.execute(
    r"SELECT  ROUND((SUM(TMOV.TEMPO)/60),2)AS TEMP_ELEV, EXTRACT(MONTH FROM TMOV.DT_APONT) AS MES "
    r"FROM FOCCO3I.TORDENS_MOVTO TMOV "
    r",FOCCO3I.TORDENS_ROT ROT "
    r",FOCCO3I.TORDENS TOR "
    r",FOCCO3I.TSRENG_ORDENS_PROJ PRJ "
    r"WHERE TMOV.TORDEN_ROT_ID = ROT.ID "
    r"AND ROT.ORDEM_ID = TOR.ID "
    r"AND TOR.ID = PRJ.ORDEM_ID "
    r"AND PRJ.FUNC_ID = 1461 "
    r"AND TMOV.DT_APONT BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY EXTRACT(month FROM TMOV.DT_APONT) "
    r"ORDER BY MES "
)
horaelevador = cur.fetchall()
horaelev = pd.DataFrame(horaelevador, columns=['TEMP_ELEV', 'MES'])

connection.commit()
cur.execute(
    r"SELECT  round((sum(TMOV.tempo)/60),2) AS TEMP_TOTAL, EXTRACT(month FROM TMOV.DT_APONT) AS MES "
    r"FROM FOCCO3I.TORDENS_MOVTO TMOV "
    r"WHERE TMOV.DT_APONT BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY EXTRACT(month FROM TMOV.DT_APONT) "
    r"ORDER BY MES "
)
horatotal = cur.fetchall()
horatot = pd.DataFrame(horatotal, columns=['TEMP_TOTAL', 'MES'])

connection.commit()
cur.execute(
    r"SELECT  "
    r"CASE "
    r"    WHEN CT.DESCRICAO = 'PINTURA'                   THEN 'Pintura' "
    r"    WHEN CT.DESCRICAO = 'PRE_MONTAGEM'              THEN 'P. Mont.' "
    r"    WHEN CT.DESCRICAO = 'PREPARACAO'                THEN 'Prep.' "
    r"    WHEN CT.DESCRICAO = 'PREPARACAO_PLASMA'         THEN 'Plasma' "
    r"    WHEN CT.DESCRICAO = 'PREPARACAO_PUNCIONADEIRA'  THEN 'Punc.' "
    r"    WHEN CT.DESCRICAO = 'PREP_SUPERFICIE'           THEN 'P. Super.' "
    r"    WHEN CT.DESCRICAO = 'SOLDAGEM'                  THEN 'Solda' "
    r"    WHEN CT.DESCRICAO = 'USINAGEM'                  THEN 'Usinagem' "
    r"    WHEN CT.DESCRICAO = 'PREPARACAO_FICEP'          THEN 'FICEP' "
    r"    ELSE 'DASDA' "
    r"END DESCRICAO "
    r", ROUND ((sum(TMOV.tempo)/60), 2) AS TEMPOmovto, EXTRACT (month FROM TMOV.DT_APONT) as mes, EXTRACT (year FROM TMOV.DT_APONT) as ano "
    r"FROM FOCCO3I.TORDENS_MOVTO TMOV "
    r",FOCCO3I.TFUNCIONARIOS TFUN "
    r",FOCCO3I.TORDENS_ROT TROT "
    r",FOCCO3I.TOPERACAO TOP "
    r",FOCCO3I.TORDENS TOR "
    r",FOCCO3I.TORD_ROT_FAB_MAQ TFAB "
    r",FOCCO3I.TMAQUINAS TMAQ "
    r",FOCCO3I.TCENTROS_TRAB CT "
    r"WHERE TMOV.FUNC_ID = TFUN.ID "
    r"AND TMOV.TORDEN_ROT_ID = TROT.ID "
    r"AND TFAB.TORDEN_ROT_ID = TROT.ID "
    r"AND TMAQ.ID = TFAB.MAQUINA_ID "
    r"AND TROT.ORDEM_ID = TOR.ID "
    r"AND TROT.OPERACAO_ID = TOP.ID "
    r"AND TFAB.TORDEN_ROT_ID = TROT.ID "
    r"AND TROT.CENTR_TRAB_ID = CT.ID "
    r"AND CT.DESCRICAO IN ('PINTURA','PRE_MONTAGEM', 'PREPARACAO', 'PREPARACAO_PLASMA','PREPARACAO_PUNCIONADEIRA','PREP_SUPERFICIE','SOLDAGEM', 'USINAGEM','PREPARACAO_FICEP' )   "
    r"AND TMOV.DT_APONT BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY CT.DESCRICAO, EXTRACT (month FROM TMOV.DT_APONT), EXTRACT (year FROM TMOV.DT_APONT) "
    r"ORDER BY MES "
)
apont = cur.fetchall()
apont = pd.DataFrame(apont, columns=['DESCRICAO', 'TEMPOMOVTO', 'MES', 'ano'])

connection.commit()
cur.execute(
    r"SELECT  "
    r"CASE "
    r"    WHEN CC.DESCRICAO40 = 'Pintura'                                         THEN 'Pintura' "
    r"    WHEN CC.DESCRICAO40 = 'Pre Montagem'                                    THEN 'P. Mont.' "
    r"    WHEN CC.DESCRICAO40 like '%Prepara%' AND CC.DESCRICAO40 NOT LIKE '% %'  THEN 'Prep.' "
    r"    WHEN CC.DESCRICAO40 like '% Plasma'                                     THEN 'Plasma' "
    r"    WHEN CC.DESCRICAO40 like '% Puncionadeira'                              THEN 'Punc.' "
    r"    WHEN CC.DESCRICAO40 like '% de Superf%'                                 THEN 'P. Super.' "
    r"    WHEN CC.DESCRICAO40 = 'Soldagem'                                        THEN 'Solda' "
    r"    WHEN CC.DESCRICAO40 = 'Usinagem'                                        THEN 'Usinagem' "
    r"    ELSE 'DASDA' "
    r"END DESCRICAO40 "
    r", ROUND (SUM ((PTO.MINUTOSTOTAL)/60)*0.8,2) as tempodisp, EXTRACT (month FROM PTO.DATAAPURACAO) as mes, EXTRACT (year FROM PTO.DATAAPURACAO) as ano "
    r"FROM META.RHCENTROSCUSTO1 CC "
    r",META.RHSETORES SETO "
    r",META.RHCONTRATOS CON  "
    r",META.RHPESSOAS PESS "
    r",META.RHLANCTOSEVENTOSPTO PTO "
    r"WHERE SETO.CENTROCUSTO1 = CC.CENTROCUSTO1 "
    r"AND CON.SETOR = SETO.SETOR "
    r"AND PESS.PESSOA = CON.PESSOA "
    r"AND PTO.CONTRATO = CON.CONTRATO "
    r"AND PTO.EVENTOPONTO IN (1, 121, 122) "
    r"AND CC.DESCRICAO40 IN ('Soldagem','Pintura','Preparação de Superfície','Usinagem', 'Preparação', 'Pre Montagem', 'Preparação Puncionadeira','Preparação Plasma') "
    r"AND PTO.DATAAPURACAO BETWEEN TO_DATE ('01', 'MM') AND TO_DATE (SYSDATE -1) "
    r"GROUP BY CC.DESCRICAO40 ,EXTRACT (MONTH FROM PTO.DATAAPURACAO), EXTRACT (year FROM PTO.DATAAPURACAO) "
    r"ORDER BY MES "
)
hdisp = cur.fetchall()
hdisp = pd.DataFrame(hdisp, columns=['DESCRICAO40', 'TEMPODISP', 'MES', 'ano'])


linha = [
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 1, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 2, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 3, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 4, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 5, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 6, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 7, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 8, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 9, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 10, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 11, 'ano': 2023},
    {'DESCRICAO40': 'FICEP', 'TEMPODISP': 154.88, 'MES': 12, 'ano': 2023},
]
hdisp = hdisp._append(linha, ignore_index=True)
logo = Image.open("sr-engenharia.png")
currentMonth = datetime.now().month
hdisp['TEMPOMOVTO'] = ''

for i in hdisp.index:
    pana = True
    for j in apont.index:
        if (hdisp.iloc[i]['DESCRICAO40'] == apont.iloc[j]['DESCRICAO']) and (hdisp.iloc[i]['MES'] == apont.iloc[j]['MES']) and (hdisp.iloc[i]['ano'] == apont.iloc[j]['ano']):
            hdisp.at[i, 'TEMPOMOVTO'] = apont.loc[j]['TEMPOMOVTO']
            pana = False
            break

    if pana:
        hdisp.at[i, 'TEMPOMOVTO'] = 0

app = Dash(__name__)

app.layout = html.Div([
    html.H1(
        'COMPARATIVO DE HORAS'
    ),
    dcc.Slider(1, 12, 1, marks={
        1: {'label': 'JAN 2023'},
        2: {'label': 'FEV 2023'},
        3: {'label': 'MAR 2023'},
        4: {'label': 'ABR 2023'},
        5: {'label': 'MAI 2023'},
        6: {'label': 'JUN 2023'},
        7: {'label': 'JUL 2023'},
        8: {'label': 'AGO 2023'},
        9: {'label': 'SET 2023'},
        10: {'label': 'OUT 2023'},
        11: {'label': 'NOV 2023'},
        12: {'label': 'DEZ 2023'},
    },
               id='meu_slider',
               value=currentMonth - 1,
               included=False
               ),
    dcc.Graph(id='meu_grafico',
              config={'displayModeBar': False}
              )
])


@app.callback(
    Output('meu_grafico', 'figure'),
    Input('meu_slider', 'value')
)
def update_figure(mes_selecionado):
    hdispfilt = (hdisp[hdisp['MES'] == mes_selecionado])
    fig = go.Figure(data=[
        go.Bar(name='Tempo de OP Apontadas',
               x=hdispfilt['DESCRICAO40'],
               y=hdispfilt['TEMPOMOVTO'],
               text=hdispfilt['TEMPOMOVTO'],
               textposition='outside'
               ),
        go.Bar(name='Tempo Disponivel',
               x=hdispfilt['DESCRICAO40'],
               y=hdispfilt['TEMPODISP'],
               text=hdispfilt['TEMPODISP'],
               textposition='outside'
               )
    ])
    fig.update_layout(
        xaxis={'categoryorder': 'total descending'},
        barmode='group',
        width=1300,
        height=800
    ),
    fig.add_layout_image(
        dict(
            source=logo,
            xref="paper",
            yref="paper",
            x=0.95,
            y=0.85,
            sizex=0.15,
            sizey=0.15,
            xanchor="right",
            yanchor="bottom"
        )
    ),
    return fig


if __name__ == '__main__':
    app.run(host='10.40.3.48', port=8080)
