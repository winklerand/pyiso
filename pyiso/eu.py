from pyiso.base import BaseClient
from pyiso import LOGGER
import requests
import pandas as pd
import numpy as np
from io import StringIO
from time import sleep
from datetime import datetime, timedelta
import pytz
from os import environ


class EUClient(BaseClient):
    NAME = 'EU'
    TZ_NAME = 'UTC'
    base_url = 'https://transparency.entsoe.eu/'
    export_endpoint = 'load-domain/r2/totalLoadR2/export'

    CONTROL_AREAS = {
        'AL': {'country': 'Albania', 'Code': 'CTA|AL',
            'ENTSOe_ID': 'CTY|10YAL-KESH-----5!CTA|10YAL-KESH-----5'},
        'AT': {'country': 'Austria', 'Code': 'CTA|AT',
            'ENTSOe_ID': 'CTY|10YAT-APG------L!CTA|10YAT-APG------L'},
        'BE': {'country': 'Belgium', 'Code': 'CTA|BE',
            'ENTSOe_ID': 'CTY|10YBE----------2!CTA|10YBE----------2'},
        'BA': {'country': 'Bosnia and Herz. ', 'Code': 'CTA|BA',
            'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!CTA|10YBA-JPCC-----D'},
        'BG': {'country': 'Bulgaria', 'Code': 'CTA|BG',
            'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!CTA|10YCA-BULGARIA-R'},
        'HR': {'country': 'Croatia', 'Code': 'CTA|HR',
            'ENTSOe_ID': 'CTY|10YHR-HEP------M!CTA|10YHR-HEP------M'},
        'CY': {'country': 'Cyprus', 'Code': 'CTA|CY',
            'ENTSOe_ID': 'CTY|10YCY-1001A0003J!CTA|10YCY-1001A0003J'},
        'CZ': {'country': 'Czech Republic', 'Code': 'CTA|CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YCZ-CEPS-----N'},
        'PL-CZ': {'country': 'Czech Republic', 'Code': 'CTA|PL-CZ',
            'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!CTA|10YDOM-1001A082L'},
        'DK': {'country': 'Denmark', 'Code': 'CTA|DK',
            'ENTSOe_ID': 'CTY|10Y1001A1001A65H!CTA|10Y1001A1001A796'},
        'EE': {'country': 'Estonia', 'Code': 'CTA|EE',
            'ENTSOe_ID': 'CTY|10Y1001A1001A39I!CTA|10Y1001A1001A39I'},
        'MK': {'country': 'FYR Macedonia', 'Code': 'CTA|MK',
            'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!CTA|10YMK-MEPSO----8'},
        'FI': {'country': 'Finland', 'Code': 'CTA|FI',
            'ENTSOe_ID': 'CTY|10YFI-1--------U!CTA|10YFI-1--------U'},
        'FR': {'country': 'France', 'Code': 'CTA|FR',
            'ENTSOe_ID': 'CTY|10YFR-RTE------C!CTA|10YFR-RTE------C'},
        'DE(50HzT)': {'country': 'Germany', 'Code': 'CTA|DE(50HzT)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-VE-------2'},
        'DE(Amprion)': {'country': 'Germany', 'Code': 'CTA|DE(Amprion)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-RWENET---I'},
        'DE(TenneT GER)': {'country': 'Germany', 'Code': 'CTA|DE(TenneT GER)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-EON------1&'},
        'DE(TransnetBW)': {'country': 'Germany', 'Code': 'CTA|DE(TransnetBW)',
            'ENTSOe_ID': 'CTY|10Y1001A1001A83F!CTA|10YDE-ENBW-----N'},
        'GR': {'country': 'Greece', 'Code': 'CTA|GR',
            'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!CTA|10YGR-HTSO-----Y'},
        'HU': {'country': 'Hungary', 'Code': 'CTA|HU',
            'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!CTA|10YHU-MAVIR----U'},
        'IE': {'country': 'Ireland', 'Code': 'CTA|IE',
            'ENTSOe_ID': 'CTY|10YIE-1001A00010!CTA|10YIE-1001A00010'},
        'IT': {'country': 'Italy', 'Code': 'CTA|IT',
            'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!CTA|10YIT-GRTN-----B'},
        'LV': {'country': 'Latvia', 'Code': 'CTA|LV',
            'ENTSOe_ID': 'CTY|10YLV-1001A00074!CTA|10YLV-1001A00074'},
        'LT': {'country': 'Lithuania', 'Code': 'CTA|LT',
            'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!CTA|10YLT-1001A0008Q'},
        'LU': {'country': 'Luxembourg', 'Code': 'CTA|LU',
            'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!CTA|10YLU-CEGEDEL-NQ'},
        'MT': {'country': 'Malta', 'Code': 'CTA|MT',
            'ENTSOe_ID': 'CTY|MT!CTA|Not+delivered+MT'},
        'MD': {'country': 'Moldavia', 'Code': 'CTA|MD',
            'ENTSOe_ID': 'CTY|MD!CTA|Not+delivered+MD'},
        'ME': {'country': 'Montenegro', 'Code': 'CTA|ME',
            'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!CTA|10YCS-CG-TSO---S'},
        'NL': {'country': 'Netherlands', 'Code': 'CTA|NL',
            'ENTSOe_ID': 'CTY|10YNL----------L!CTA|10YNL----------L'},
        'NO': {'country': 'Norway', 'Code': 'CTA|NO',
            'ENTSOe_ID': 'CTY|10YNO-0--------C!CTA|10YNO-0--------C'},
        'PL': {'country': 'Poland', 'Code': 'CTA|PL',
            'ENTSOe_ID': 'CTY|10YPL-AREA-----S!CTA|10YPL-AREA-----S'},
        'PT': {'country': 'Portugal', 'Code': 'CTA|PT',
            'ENTSOe_ID': 'CTY|10YPT-REN------W!CTA|10YPT-REN------W'},
        'RO': {'country': 'Romania', 'Code': 'CTA|RO',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
        'RU': {'country': 'Russia', 'Code': 'CTA|RU',
            'ENTSOe_ID': 'CTY|10YRO-TEL------P!CTA|10YRO-TEL------P'},
        'RU-KGD': {'country': 'Russia', 'Code': 'CTA|RU-KGD',
            'ENTSOe_ID': 'CTY|RU!CTA|10Y1001A1001A50U'},
        'RS': {'country': 'Serbia', 'Code': 'CTA|RS',
            'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!CTA|10YCS-SERBIATSOV'},
        'SK': {'country': 'Slovakia', 'Code': 'CTA|SK',
            'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!CTA|10YSK-SEPS-----K'},
        'SI': {'country': 'Slovenia', 'Code': 'CTA|SI',
            'ENTSOe_ID': 'CTY|10YSI-ELES-----O!CTA|10YSI-ELES-----O'},
        'ES': {'country': 'Spain', 'Code': 'CTA|ES',
            'ENTSOe_ID': 'CTY|10YES-REE------0!CTA|10YES-REE------0'},
        'SE': {'country': 'Sweden', 'Code': 'CTA|SE',
            'ENTSOe_ID': 'CTY|10YSE-1--------K!CTA|10YSE-1--------K'},
        'CH': {'country': 'Switzerland', 'Code': 'CTA|CH',
            'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!CTA|10YCH-SWISSGRIDZ'},
        'TR': {'country': 'Turkey', 'Code': 'CTA|TR',
            'ENTSOe_ID': 'CTY|TR!CTA|10YTR-TEIAS----W'},
        'UA': {'country': 'Ukraine', 'Code': 'CTA|UA',
            'ENTSOe_ID': 'CTY|UA!CTA|10Y1001A1001A869'},
        'UA-WEPS': {'country': 'Ukraine', 'Code': 'CTA|UA-WEPS',
            'ENTSOe_ID': 'CTY|UA!CTA|10YUA-WEPS-----0'},
        'NIE': {'country': 'United Kingdom', 'Code': 'CTA|NIE',
            'ENTSOe_ID': 'CTY|GB!CTA|10Y1001A1001A016'},
        'National Grid': {'country': 'United Kingdom', 'Code': 'CTA|National Grid',
            'ENTSOe_ID': 'CTY|GB!CTA|10YGB----------A'},
        }

    BIDDING_ZONES = {
        'AL': {'Code': 'BZN|AL', 'country': 'Albania', 'ENTSOe_ID': 'CTY|10YAL-KESH-----5!BZN|10YAL-KESH-----5'},
        'BA': {'Code': 'BZN|BA', 'country': 'Bosnia and Herz.',
               'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!BZN|10YBA-JPCC-----D'},
        'BE': {'Code': 'BZN|BE', 'country': 'Belgium', 'ENTSOe_ID': 'CTY|10YBE----------2!BZN|10YBE----------2'},
        'BG': {'Code': 'BZN|BG', 'country': 'Bulgaria', 'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!BZN|10YCA-BULGARIA-R'},
        'BY': {'Code': 'BZN|BY', 'country': 'Belarus', 'ENTSOe_ID': 'CTY|BY!BZN|10Y1001A1001A51S'},
        'CH': {'Code': 'BZN|CH', 'country': 'Switzerland', 'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!BZN|10YCH-SWISSGRIDZ'},
        'CY': {'Code': 'BZN|CY', 'country': 'Cyprus', 'ENTSOe_ID': 'CTY|10YCY-1001A0003J!BZN|10YCY-1001A0003J'},
        'CZ': {'Code': 'BZN|CZ', 'country': 'Czech Republic', 'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!BZN|10YCZ-CEPS-----N'},
        'CZ+DE+SK': {'Code': 'BZN|CZ+DE+SK', 'country': 'Slovakia',
                     'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!BZN|10YDOM-CZ-DE-SKK'},
        'DE-AT-LU': {'Code': 'BZN|DE-AT-LU', 'country': 'Luxembourg',
                     'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!BZN|10Y1001A1001A63L'},
        'DK1': {'Code': 'BZN|DK1', 'country': 'Denmark', 'ENTSOe_ID': 'CTY|10Y1001A1001A65H!BZN|10YDK-1--------W'},
        'DK2': {'Code': 'BZN|DK2', 'country': 'Denmark', 'ENTSOe_ID': 'CTY|10Y1001A1001A65H!BZN|10YDK-2--------M'},
        'EE': {'Code': 'BZN|EE', 'country': 'Estonia', 'ENTSOe_ID': 'CTY|10Y1001A1001A39I!BZN|10Y1001A1001A39I'},
        'ES': {'Code': 'BZN|ES', 'country': 'Spain', 'ENTSOe_ID': 'CTY|10YES-REE------0!BZN|10YES-REE------0'},
        'FI': {'Code': 'BZN|FI', 'country': 'Finland', 'ENTSOe_ID': 'CTY|10YFI-1--------U!BZN|10YFI-1--------U'},
        'FR': {'Code': 'BZN|FR', 'country': 'France', 'ENTSOe_ID': 'CTY|10YFR-RTE------C!BZN|10YFR-RTE------C'},
        'GB': {'Code': 'BZN|GB', 'country': 'United Kingdom', 'ENTSOe_ID': 'CTY|GB!BZN|10YGB----------A'},
        'GR': {'Code': 'BZN|GR', 'country': 'Greece', 'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!BZN|10YGR-HTSO-----Y'},
        'HR': {'Code': 'BZN|HR', 'country': 'Croatia', 'ENTSOe_ID': 'CTY|10YHR-HEP------M!BZN|10YHR-HEP------M'},
        'HU': {'Code': 'BZN|HU', 'country': 'Hungary', 'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!BZN|10YHU-MAVIR----U'},
        'IE(SEM)': {'Code': 'BZN|IE(SEM)', 'country': 'United Kingdom', 'ENTSOe_ID': 'CTY|GB!BZN|10Y1001A1001A59C',
                    'da_market': 'DAHH', 'da_frequency': '30m'},
        'IT-Brindisi': {'Code': 'BZN|IT-Brindisi', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A699'},
        'IT-Centre-North': {'Code': 'BZN|IT-Centre-North', 'country': 'Italy',
                            'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A70O'},
        'IT-Centre-South': {'Code': 'BZN|IT-Centre-South', 'country': 'Italy',
                            'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A71M'},
        'IT-Foggia': {'Code': 'BZN|IT-Foggia', 'country': 'Italy',
                      'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A72K'},
        'IT-GR': {'Code': 'BZN|IT-GR', 'country': 'Italy', 'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A66F'},
        'IT-Malta': {'Code': 'BZN|IT-Malta', 'country': 'Italy',
                     'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A877'},
        'IT-North': {'Code': 'BZN|IT-North', 'country': 'Italy',
                     'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A73I'},
        'IT-North-AT': {'Code': 'BZN|IT-North-AT', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A80L'},
        'IT-North-CH': {'Code': 'BZN|IT-North-CH', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A68B'},
        'IT-North-FR': {'Code': 'BZN|IT-North-FR', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A81J'},
        'IT-North-SI': {'Code': 'BZN|IT-North-SI', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A67D'},
        'IT-Priolo': {'Code': 'BZN|IT-Priolo', 'country': 'Italy',
                      'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A76C'},
        'IT-Rossano': {'Code': 'BZN|IT-Rossano', 'country': 'Italy',
                       'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A77A'},
        'IT-SACOAC': {'Code': 'BZN|IT-SACOAC', 'country': 'Italy',
                      'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A885'},
        'IT-SACODC': {'Code': 'BZN|IT-SACODC', 'country': 'Italy',
                      'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A893'},
        'IT-Sardinia': {'Code': 'BZN|IT-Sardinia', 'country': 'Italy',
                        'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A74G'},
        'IT-Sicily': {'Code': 'BZN|IT-Sicily', 'country': 'Italy',
                      'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A75E'},
        'IT-South': {'Code': 'BZN|IT-South', 'country': 'Italy',
                     'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!BZN|10Y1001A1001A788'},
        'LT': {'Code': 'BZN|LT', 'country': 'Lithuania', 'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!BZN|10YLT-1001A0008Q'},
        'LV': {'Code': 'BZN|LV', 'country': 'Latvia', 'ENTSOe_ID': 'CTY|10YLV-1001A00074!BZN|10YLV-1001A00074'},
        'MD': {'Code': 'BZN|MD', 'country': 'Moldova', 'ENTSOe_ID': 'CTY|MD!BZN|Not delivered MD'},
        'ME': {'Code': 'BZN|ME', 'country': 'Montenegro', 'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!BZN|10YCS-CG-TSO---S'},
        'MK': {'Code': 'BZN|MK', 'country': 'FYR Macedonia', 'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!BZN|10YMK-MEPSO----8'},
        'MT': {'Code': 'BZN|MT', 'country': 'Malta', 'ENTSOe_ID': 'CTY|10Y1001A1001A93C!BZN|10Y1001A1001A93C'},
        'NL': {'Code': 'BZN|NL', 'country': 'Netherlands', 'ENTSOe_ID': 'CTY|10YNL----------L!BZN|10YNL----------L'},
        'NO1': {'Code': 'BZN|NO1', 'country': 'Norway', 'ENTSOe_ID': 'CTY|10YNO-0--------C!BZN|10YNO-1--------2'},
        'NO2': {'Code': 'BZN|NO2', 'country': 'Norway', 'ENTSOe_ID': 'CTY|10YNO-0--------C!BZN|10YNO-2--------T'},
        'NO3': {'Code': 'BZN|NO3', 'country': 'Norway', 'ENTSOe_ID': 'CTY|10YNO-0--------C!BZN|10YNO-3--------J'},
        'NO4': {'Code': 'BZN|NO4', 'country': 'Norway', 'ENTSOe_ID': 'CTY|10YNO-0--------C!BZN|10YNO-4--------9'},
        'NO5': {'Code': 'BZN|NO5', 'country': 'Norway', 'ENTSOe_ID': 'CTY|10YNO-0--------C!BZN|10Y1001A1001A48H'},
        'PL': {'Code': 'BZN|PL', 'country': 'Poland', 'ENTSOe_ID': 'CTY|10YPL-AREA-----S!BZN|10YPL-AREA-----S'},
        'PT': {'Code': 'BZN|PT', 'country': 'Portugal', 'ENTSOe_ID': 'CTY|10YPT-REN------W!BZN|10YPT-REN------W'},
        'RO': {'Code': 'BZN|RO', 'country': 'Romania', 'ENTSOe_ID': 'CTY|10YRO-TEL------P!BZN|10YRO-TEL------P'},
        'RS': {'Code': 'BZN|RS', 'country': 'Serbia', 'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!BZN|10YCS-SERBIATSOV'},
        'RU': {'Code': 'BZN|RU', 'country': 'Russia', 'ENTSOe_ID': 'CTY|RU!BZN|10Y1001A1001A49F'},
        'RU-KGD': {'Code': 'BZN|RU-KGD', 'country': 'Russia', 'ENTSOe_ID': 'CTY|RU!BZN|10Y1001A1001A50U'},
        'SE1': {'Code': 'BZN|SE1', 'country': 'Sweden', 'ENTSOe_ID': 'CTY|10YSE-1--------K!BZN|10Y1001A1001A44P'},
        'SE2': {'Code': 'BZN|SE2', 'country': 'Sweden', 'ENTSOe_ID': 'CTY|10YSE-1--------K!BZN|10Y1001A1001A45N'},
        'SE3': {'Code': 'BZN|SE3', 'country': 'Sweden', 'ENTSOe_ID': 'CTY|10YSE-1--------K!BZN|10Y1001A1001A46L'},
        'SE4': {'Code': 'BZN|SE4', 'country': 'Sweden', 'ENTSOe_ID': 'CTY|10YSE-1--------K!BZN|10Y1001A1001A47J'},
        'SI': {'Code': 'BZN|SI', 'country': 'Slovenia', 'ENTSOe_ID': 'CTY|10YSI-ELES-----O!BZN|10YSI-ELES-----O'},
        'SK': {'Code': 'BZN|SK', 'country': 'Slovakia', 'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!BZN|10YSK-SEPS-----K'},
        'TR': {'Code': 'BZN|TR', 'country': 'Turkey', 'ENTSOe_ID': 'CTY|TR!BZN|10YTR-TEIAS----W'},
        'UA': {'Code': 'BZN|UA', 'country': 'Ukraine', 'ENTSOe_ID': 'CTY|UA!BZN|10Y1001A1001A869'},
        'UA-WEPS': {'Code': 'BZN|UA-WEPS', 'country': 'Ukraine', 'ENTSOe_ID': 'CTY|UA!BZN|10YUA-WEPS-----0'}
    }

    MARKET_BALANCING_AREAS = {
        'AL': {'country': 'Albania', 'Code': 'MBA|AL', 'ENTSOe_ID': 'CTY|10YAL-KESH-----5!MBA|10YAL-KESH-----5'},
        'AT': {'country': 'Austria', 'Code': 'MBA|AT', 'ENTSOe_ID': 'CTY|10YAT-APG------L!MBA|10YAT-APG------L'},
        'BA': {'country': 'Bosnia and Herz.', 'Code': 'MBA|BA',
               'ENTSOe_ID': 'CTY|10YBA-JPCC-----D!MBA|10YBA-JPCC-----D'},
        'BE': {'country': 'Belgium', 'Code': 'MBA|BE', 'ENTSOe_ID': 'CTY|10YBE----------2!MBA|10YBE----------2'},
        'BG': {'country': 'Bulgaria', 'Code': 'MBA|BG', 'ENTSOe_ID': 'CTY|10YCA-BULGARIA-R!MBA|10YCA-BULGARIA-R'},
        'BY': {'country': 'Belarus', 'Code': 'MBA|BY', 'ENTSOe_ID': 'CTY|BY!MBA|10Y1001A1001A51S'},
        'CH': {'country': 'Switzerland', 'Code': 'MBA|CH', 'ENTSOe_ID': 'CTY|10YCH-SWISSGRIDZ!MBA|10YCH-SWISSGRIDZ'},
        'CY': {'country': 'Cyprus', 'Code': 'MBA|CY', 'ENTSOe_ID': 'CTY|10YCY-1001A0003J!MBA|10YCY-1001A0003J'},
        'CZ': {'country': 'Czech Republic', 'Code': 'MBA|CZ', 'ENTSOe_ID': 'CTY|10YCZ-CEPS-----N!MBA|10YCZ-CEPS-----N'},
        'DE-LU': {'country': 'Luxembourg', 'Code': 'MBA|DE-LU',
                  'ENTSOe_ID': 'CTY|10YLU-CEGEDEL-NQ!MBA|10Y1001A1001A82H'},
        'DK1': {'country': 'Denmark', 'Code': 'MBA|DK1', 'ENTSOe_ID': 'CTY|10Y1001A1001A65H!MBA|10YDK-1--------W'},
        'DK2': {'country': 'Denmark', 'Code': 'MBA|DK2', 'ENTSOe_ID': 'CTY|10Y1001A1001A65H!MBA|10YDK-2--------M'},
        'EE': {'country': 'Estonia', 'Code': 'MBA|EE', 'ENTSOe_ID': 'CTY|10Y1001A1001A39I!MBA|10Y1001A1001A39I'},
        'ES': {'country': 'Spain', 'Code': 'MBA|ES', 'ENTSOe_ID': 'CTY|10YES-REE------0!MBA|10YES-REE------0'},
        'FI': {'country': 'Finland', 'Code': 'MBA|FI', 'ENTSOe_ID': 'CTY|10YFI-1--------U!MBA|10YFI-1--------U'},
        'FR': {'country': 'France', 'Code': 'MBA|FR', 'ENTSOe_ID': 'CTY|10YFR-RTE------C!MBA|10YFR-RTE------C'},
        'GB': {'country': 'United Kingdom', 'Code': 'MBA|GB', 'ENTSOe_ID': 'CTY|GB!MBA|10YGB----------A'},
        'GR': {'country': 'Greece', 'Code': 'MBA|GR', 'ENTSOe_ID': 'CTY|10YGR-HTSO-----Y!MBA|10YGR-HTSO-----Y'},
        'HR': {'country': 'Croatia', 'Code': 'MBA|HR', 'ENTSOe_ID': 'CTY|10YHR-HEP------M!MBA|10YHR-HEP------M'},
        'HU': {'country': 'Hungary', 'Code': 'MBA|HU', 'ENTSOe_ID': 'CTY|10YHU-MAVIR----U!MBA|10YHU-MAVIR----U'},
        'IE(SEM)': {'country': 'United Kingdom', 'Code': 'MBA|IE(SEM)', 'ENTSOe_ID': 'CTY|GB!MBA|10Y1001A1001A59C'},
        'IT': {'country': 'Italy', 'Code': 'MBA|IT', 'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!MBA|10YIT-GRTN-----B'},
        'IT-MACRZONENORTH': {'country': 'Italy', 'Code': 'MBA|IT-MACRZONENORTH',
                             'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!MBA|10Y1001A1001A84D'},
        'IT-MACRZONESOUTH': {'country': 'Italy', 'Code': 'MBA|IT-MACRZONESOUTH',
                             'ENTSOe_ID': 'CTY|10YIT-GRTN-----B!MBA|10Y1001A1001A85B'},
        'LT': {'country': 'Lithuania', 'Code': 'MBA|LT', 'ENTSOe_ID': 'CTY|10YLT-1001A0008Q!MBA|10YLT-1001A0008Q'},
        'LV': {'country': 'Latvia', 'Code': 'MBA|LV', 'ENTSOe_ID': 'CTY|10YLV-1001A00074!MBA|10YLV-1001A00074'},
        'MD': {'country': 'Moldova', 'Code': 'MBA|MD', 'ENTSOe_ID': 'CTY|MD!MBA|Not delivered MD'},
        'ME': {'country': 'Montenegro', 'Code': 'MBA|ME', 'ENTSOe_ID': 'CTY|10YCS-CG-TSO---S!MBA|10YCS-CG-TSO---S'},
        'MK': {'country': 'FYR Macedonia', 'Code': 'MBA|MK', 'ENTSOe_ID': 'CTY|10YMK-MEPSO----8!MBA|10YMK-MEPSO----8'},
        'MT': {'country': 'Malta', 'Code': 'MBA|MT', 'ENTSOe_ID': 'CTY|10Y1001A1001A93C!MBA|10Y1001A1001A93C'},
        'NL': {'country': 'Netherlands', 'Code': 'MBA|NL', 'ENTSOe_ID': 'CTY|10YNL----------L!MBA|10YNL----------L'},
        'NO': {'country': 'Norway', 'Code': 'MBA|NO', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10YNO-0--------C'},
        'NO1': {'country': 'Norway', 'Code': 'MBA|NO1', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10YNO-1--------2'},
        'NO2': {'country': 'Norway', 'Code': 'MBA|NO2', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10YNO-2--------T'},
        'NO3': {'country': 'Norway', 'Code': 'MBA|NO3', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10YNO-3--------J'},
        'NO4': {'country': 'Norway', 'Code': 'MBA|NO4', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10YNO-4--------9'},
        'NO5': {'country': 'Norway', 'Code': 'MBA|NO5', 'ENTSOe_ID': 'CTY|10YNO-0--------C!MBA|10Y1001A1001A48H'},
        'PL': {'country': 'Poland', 'Code': 'MBA|PL', 'ENTSOe_ID': 'CTY|10YPL-AREA-----S!MBA|10YPL-AREA-----S'},
        'PT': {'country': 'Portugal', 'Code': 'MBA|PT', 'ENTSOe_ID': 'CTY|10YPT-REN------W!MBA|10YPT-REN------W'},
        'RO': {'country': 'Romania', 'Code': 'MBA|RO', 'ENTSOe_ID': 'CTY|10YRO-TEL------P!MBA|10YRO-TEL------P'},
        'RS': {'country': 'Serbia', 'Code': 'MBA|RS', 'ENTSOe_ID': 'CTY|10YCS-SERBIATSOV!MBA|10YCS-SERBIATSOV'},
        'RU': {'country': 'Russia', 'Code': 'MBA|RU', 'ENTSOe_ID': 'CTY|RU!MBA|10Y1001A1001A49F'},
        'RU-KGD': {'country': 'Russia', 'Code': 'MBA|RU-KGD', 'ENTSOe_ID': 'CTY|RU!MBA|10Y1001A1001A50U'},
        'SE': {'country': 'Sweden', 'Code': 'MBA|SE', 'ENTSOe_ID': 'CTY|10YSE-1--------K!MBA|10YSE-1--------K'},
        'SE1': {'country': 'Sweden', 'Code': 'MBA|SE1', 'ENTSOe_ID': 'CTY|10YSE-1--------K!MBA|10Y1001A1001A44P'},
        'SE2': {'country': 'Sweden', 'Code': 'MBA|SE2', 'ENTSOe_ID': 'CTY|10YSE-1--------K!MBA|10Y1001A1001A45N'},
        'SE3': {'country': 'Sweden', 'Code': 'MBA|SE3', 'ENTSOe_ID': 'CTY|10YSE-1--------K!MBA|10Y1001A1001A46L'},
        'SE4': {'country': 'Sweden', 'Code': 'MBA|SE4', 'ENTSOe_ID': 'CTY|10YSE-1--------K!MBA|10Y1001A1001A47J'},
        'SI': {'country': 'Slovenia', 'Code': 'MBA|SI', 'ENTSOe_ID': 'CTY|10YSI-ELES-----O!MBA|10YSI-ELES-----O'},
        'SK': {'country': 'Slovakia', 'Code': 'MBA|SK', 'ENTSOe_ID': 'CTY|10YSK-SEPS-----K!MBA|10YSK-SEPS-----K'},
        'TR': {'country': 'Turkey', 'Code': 'MBA|TR', 'ENTSOe_ID': 'CTY|TR!MBA|10YTR-TEIAS----W'},
        'UA': {'country': 'Ukraine', 'Code': 'MBA|UA', 'ENTSOe_ID': 'CTY|UA!MBA|10Y1001A1001A869'},
        'UA-WEPS': {'country': 'Ukraine', 'Code': 'MBA|UA-WEPS', 'ENTSOe_ID': 'CTY|UA!MBA|10YUA-WEPS-----0'}
    }

    def get_load(self, control_area=None, latest=False, start_at=None, end_at=None,
                 forecast=False, **kwargs):
        self.handle_options(data='load', start_at=start_at, end_at=end_at, forecast=forecast,
                            latest=latest, control_area=control_area, **kwargs)

        pieces = []
        for date in self.dates():
            payload = self.construct_payload(date)
            url = self.base_url + self.export_endpoint
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_load_response(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)


    def get_day_ahead_price(self, bidding_zone=None, latest=False, start_at=None, end_at=None, **kwargs):
        self.handle_options(data='day_ahead_price', start_at=start_at, end_at=end_at,
                            latest=latest, bidding_zone=bidding_zone, forecast=False, **kwargs)
        try:
            self.options['market'] = self.BIDDING_ZONES[bidding_zone]['da_market']
            self.options['frequency'] = self.BIDDING_ZONES[bidding_zone]['da_frequency']
        except KeyError:
            # default: hourly, market type DAHR
            # market code 'DAHR' stands for day ahead hourly?
            self.options['market'] = 'DAHR'
            self.options['frequency'] = '1hr'

        pieces = []
        export_endpoint = 'transmission-domain/r2/dayAheadPrices/export'
        for date in self.dates():
            payload = self.construct_payload_bidding_zone(date)
            url = self.base_url + export_endpoint
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_day_ahead_price(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)


    def get_imbalance(self, market_balancing_area=None, latest=False, start_at=None, end_at=None, **kwargs):
        # https://transparency.entsoe.eu/
        # name=
        # &defaultValue=true
        # &viewType=TABLE
        # &areaType=MBA
        # &atch=false
        # &dateTime.dateTime=08.01.2017+00%3A00%7CUTC%7CDAYTIMERANGE
        # &dateTime.endDateTime=08.01.2017+00%3A00%7CUTC%7CDAYTIMERANGE
        # &marketArea.values=CTY%7C10Y1001A1001A83F!MBA%7C10Y1001A1001A82H
        # &dateTime.timezone=UTC
        # &dateTime.timezone_input=UTC
        # &dataItem=ALL
        # &timeRange=DEFAULT
        # &exportType=CSV
        self.handle_options(data='imbalance', start_at=start_at, end_at=end_at,
                            latest=latest, market_balancing_area=market_balancing_area, forecast=False, **kwargs)
        try:
            self.options['market'] = self.MARKET_BALANCING_AREAS[market_balancing_area]['da_market']
            self.options['frequency'] = self.MARKET_BALANCING_AREAS[market_balancing_area]['da_frequency']
        except KeyError:
            # default: hourly, market type DAHR
            # market code 'DAHR' stands for day ahead hourly?
            self.options['market'] = 'DAHR'
            self.options['frequency'] = '1hr'

        pieces = []
        export_endpoint = 'balancing/r2/imbalance/export'
        for date in self.dates():
            payload = self.construct_payload_market_balancing_area(date)
            url = self.base_url + export_endpoint
            response = self.fetch_entsoe(url, payload)
            day_df = self.parse_imbalance(response)
            pieces.append(day_df)

        df = pd.concat(pieces)
        sliced = self.slice_times(df)
        return self.serialize_faster(sliced)


    def handle_options(self, **kwargs):
        # regular handle options
        super(EUClient, self).handle_options(**kwargs)

        # if latest is True
        if self.options.get('latest', None):
            self.options.update(start_at=datetime.now(pytz.utc) - timedelta(days=1),
                                end_at=datetime.now(pytz.utc))

        # workaround for base.handle_options setting forecast to false if end_at too far in past
        if kwargs['forecast']:
            self.options['forecast'] = True

    def auth(self):
        if not getattr(self, 'session', None):
            self.session = requests.Session()

        payload = {'username': environ['ENTSOe_USERNAME'],
                   'password': environ['ENTSOe_PASSWORD'],
                   'url': '/dashboard/show'}

        # Fake an ajax login to get the cookie
        r = self.session.post(self.base_url + 'login', params=payload,
                              headers={'X-Ajax-call': 'true'})

        msg = r.text
        if msg == 'ok':
            return True
        elif msg == 'non_exists_user_or_bad_password':
            # TODO throw error
            return 'Wrong email or password'
        elif msg == 'not_human':
            return 'This account is not allowed to access web portal'
        elif msg == 'suspended_use':
            return 'User is suspended'
        else:
            return 'Unknown error:' + str(msg)

    def fetch_entsoe(self, url, payload, count=0):
        if not getattr(self, 'session', None):
            self.auth()

        r = self.request(url, params=payload)
        # TODO error checking
        if (len(r.text) == 0) or (r.status_code == 500):
            # ENTSOe responds with Error 500: "The amount of allowed requests from your IP exceed the limit."
            # in case of too many subsequent requests

            if count > 3:  # try 3 times to get response
                LOGGER.warn('Request failed, no response found after %i attempts' % count)
                return False
            # throttled
            sleep(5)
            return self.fetch_entsoe(url, payload, count + 1)
        if 'UNKNOWN_EXCEPTION' in r.text:
            LOGGER.warn('UNKNOWN EXCEPTION')
            return False
        return r.text

    def construct_payload(self, date):
        # format date
        format_str = '%d.%m.%Y'
        date_str = date.strftime(format_str) + ' 00:00|UTC|DAY'

        # TSO ID from control area code
        try:
            TSO_ID = self.CONTROL_AREAS[self.options['control_area']]['ENTSOe_ID']
        except KeyError:
            msg = 'Control area code not found for %s. Options are %s' % (self.options['control_area'],
                                                                          sorted(self.CONTROL_AREAS.keys()))
            raise ValueError(msg)

        payload = {
            'name': '',
            'defaultValue': 'false',
            'viewType': 'TABLE',
            'areaType': 'CTA',
            'atch': 'false',
            'dateTime.dateTime': date_str,
            'biddingZone.values': TSO_ID,
            'dateTime.timezone': 'UTC',
            'dateTime.timezone_input': 'UTC',
            'exportType': 'CSV',
            'dataItem': 'ALL',
            'timeRange': 'DEFAULT',
        }
        return payload


    def construct_payload_bidding_zone(self, date):
        # format date
        format_str = '%d.%m.%Y'
        date_str = date.strftime(format_str) + ' 00:00|UTC|DAY'

        # TSO ID from control area code
        try:
            TSO_ID = self.BIDDING_ZONES[self.options['bidding_zone']]['ENTSOe_ID']
        except KeyError:
            msg = 'Bidding zone code not found for %s. Options are %s' % (self.options['bidding_zone'],
                                                                          sorted(self.BIDDING_ZONES.keys()))
            raise ValueError(msg)

        payload = {
            'name': '',
            'defaultValue': 'false',
            'viewType': 'TABLE',
            'areaType': 'BZN',
            'atch': 'false',
            'dateTime.dateTime': date_str,
            'biddingZone.values': TSO_ID,
            'dateTime.timezone': 'UTC',
            'dateTime.timezone_input': 'UTC',
            'exportType': 'CSV',
            'dataItem': 'ALL',
            'timeRange': 'DEFAULT',
        }
        return payload


    def construct_payload_market_balancing_area(self, date):
        # format date
        format_str = '%d.%m.%Y'
        date_str = date.strftime(format_str) + ' 00:00|UTC|DAY'

        # TSO ID from control area code
        try:
            TSO_ID = self.MARKET_BALANCING_AREAS[self.options['market_balancing_area']]['ENTSOe_ID']
        except KeyError:
            msg = 'Market balancing area code not found for %s. Options are %s' % (self.options['market_balancing_area'],
                                                                          sorted(self.MARKET_BALANCING_AREAS.keys()))
            raise ValueError(msg)

        payload = {
            'name': '',
            'defaultValue': 'false',
            'viewType': 'TABLE',
            'atch': 'false',
            'dateTime.dateTime': date_str,
            'dateTime.timezone': 'UTC',
            'dateTime.timezone_input': 'UTC',
            'exportType': 'CSV',
            'dataItem': 'ALL',
            'timeRange': 'DEFAULT'
        }

        payload.update({
            'areaType': 'MBA',
            'marketArea.values': TSO_ID
            })

        return payload

    def parse_load_response(self, response):
        df = pd.read_csv(StringIO(response))

        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
            *df['Time (UTC)'].apply(lambda x: x.split('-')))

        # Why do these methods only work on Index and not Series?
        df.set_index(df.START_TIME_UTC, inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M ')
        df.index.set_names('timestamp', inplace=True)

        # find column name and choose which to return and which to drop
        (forecast_load_col, ) = [c for c in df.columns if 'Day-ahead Total Load Forecast [MW]' in c]
        (actual_load_col, ) = [c for c in df.columns if 'Actual Total Load [MW]' in c]
        if self.options['forecast']:
            load_col = forecast_load_col
            drop_load_col = actual_load_col
        else:
            load_col = actual_load_col
            drop_load_col = forecast_load_col

        # rename columns for list of dicts
        rename_d = {load_col: 'load_MW'}
        df.rename(columns=rename_d, inplace=True)
        drop_col = ['Time (UTC)', 'END_TIME_UTC', 'START_TIME_UTC', drop_load_col]
        df.drop(drop_col, axis=1, inplace=True)

        # drop nan rows
        df.replace('-', np.nan, inplace=True)
        df.dropna(subset=['load_MW'], inplace=True)

        # Add columns
        df['ba_name'] = self.options['control_area']
        df['freq'] = '1hr'
        df['market'] = 'RTHR'  # not necessarily appropriate terminology

        return df


    def parse_day_ahead_price(self, response):
        df = pd.read_csv(StringIO(response))

        timestamp_col = 'MTU (UTC)'
        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
            *df[timestamp_col].apply(lambda x: x.split(' - ')))

        # Why do these methods only work on Index and not Series?
        df.set_index(df.START_TIME_UTC, inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M')
        df.index.set_names('timestamp', inplace=True)

        # columns to drop
        drop_col = [timestamp_col, 'END_TIME_UTC', 'START_TIME_UTC']

        # in general, most prices are provided as numbers in a column labeled e. g. 'Day-ahead Price [EUR/MWh]'
        # however, some prices are provided as strings in a column labeled 'Day-ahead Price [Currency/MWh]', e. g. as '56.56 EUR'
        currmwh_col = 'Day-ahead Price [Currency/MWh]'
        if currmwh_col in df.columns:
            # helper function
            def split_price_and_currency(x_df):
                template_str = 'Day-ahead Price [{}/MWh]'
                currency_str = template_str.format('Currency')
                prc_and_crrncy = str(x_df[currency_str])
                if prc_and_crrncy != 'nan':
                    (prc_str, crrncy) = prc_and_crrncy.split(' ')
                    col_name = template_str.format(crrncy)
                    x_df[col_name] = float(prc_str)
                return x_df

            df = df.apply(split_price_and_currency, axis=1)
            drop_col.append(currmwh_col)

        # drop columns
        df.drop(drop_col, axis=1, inplace=True)

        # drop nan rows
        df.replace('-', np.nan, inplace=True)
        df.replace('N/A', np.nan, inplace=True)
        subset_cols = [x for x in df.columns if x != 'timestamp']
        df.dropna(subset=subset_cols, inplace=True)

        # Add columns
        df['ba_name'] = self.options['bidding_zone']
        df['freq'] = self.options['frequency']
        df['market'] = self.options['market']  # not necessarily appropriate terminology

        return df

    def parse_imbalance(self, response):
        df = pd.read_csv(StringIO(response))

        # columns: Balancing Time Unit (UTC),"+ Imbalance Price [EUR/MWh] - MBA|DE-LU","- Imbalance Price [EUR/MWh] - MBA|DE-LU","Total Imbalance [MWh] - MBA|DE-LU","Status"
        timestamp_col = 'Balancing Time Unit (UTC)'
        # get START_TIME_UTC as tz-aware datetime
        df['START_TIME_UTC'], df['END_TIME_UTC'] = zip(
            *df[timestamp_col].apply(lambda x: x.split(' - ')))
        # extract frequency from time span
        freq_timedelta = (pd.to_datetime(df['END_TIME_UTC'].iloc[0]) - pd.to_datetime(df['START_TIME_UTC'].iloc[0]))

        timedelta_to_freq = {
            pd.to_timedelta('0 days 00:15:00'): self.FREQUENCY_CHOICES.fifteenmin,
            pd.to_timedelta('0 days 00:30:00'): '30m',
            pd.to_timedelta('0 days 00:60:00'): self.FREQUENCY_CHOICES.hourly
        }
        try:
            self.options['frequency'] = timedelta_to_freq[freq_timedelta]
        except KeyError:
            self.options['frequency'] = self.FREQUENCY_CHOICES.na
        self.options['market'] = 'IMB'

        # Why do these methods only work on Index and not Series?
        df.set_index(df.START_TIME_UTC, inplace=True)
        df.index = pd.to_datetime(df.index, utc=True, format='%d.%m.%Y %H:%M')
        df.index.set_names('timestamp', inplace=True)

        # drop columns
        drop_col = [timestamp_col, 'END_TIME_UTC', 'START_TIME_UTC']
        df.drop(drop_col, axis=1, inplace=True)

        # drop nan rows
        df.replace('-', np.nan, inplace=True)
        df.replace('N/A', np.nan, inplace=True)

        # clean column names: some column names include the balancing area (ex.: 'Total Imbalance [MWh] - MBA|DE-LU')
        df.rename(columns=lambda x: str(x).partition(' -')[0], inplace=True)
        # Add columns
        df['ba_name'] = 'MBA|' + self.options['market_balancing_area']
        df['freq'] = self.options['frequency']
        df['market'] = self.options['market']  # not necessarily appropriate terminology

        return df