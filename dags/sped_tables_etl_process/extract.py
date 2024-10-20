from bs4 import BeautifulSoup
from pathlib import Path
import requests

class SpedTablesCrawler:
    def __init__(self) -> None:
        self.base_url = 'http://www.sped.fazenda.gov.br'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
            'Cookie': '',
        })
    
    def get_package_page(self) -> str:
        """
            Get page of packages
        """
        response = self.session.get(self.base_url+'/spedtabelas/AppConsulta/publico/aspx/ConsultaTabelasExternas.aspx?CodSistema=SpedFiscal')
        
        assert response.ok, 'error to get package'
        
        return response.text
    
    def get_form(self, soup_package: BeautifulSoup) -> dict:
        """
            Make form from specific required parameters
        """
        form = {}
        
        # get hidden inputs
        input_tags_list = soup_package.find_all('input', attrs={'type': 'hidden'})
        for input_tag in input_tags_list:
            form[input_tag['id']] = input_tag['value']
        
        # get another required packages
        select_tags = soup_package.find_all('select')
        form['__EVENTTARGET'] = select_tags[0]['name']
        for parent in select_tags[0].parents:
            if 'id' in parent.attrs:
                div_tag = parent
                break

        for key in form.keys():
            if 'HiddenField' in key:
                new_input = key.replace('_HiddenField', '').replace('_', '$')
                break

        form[new_input] = f'{div_tag['id'].replace('_', '$')}|{select_tags[0]['name']}'

        # fields bellow need to be setting while get the tags "option", they representing "package" and "table"
        form[select_tags[0]['name']] = ''
        form[select_tags[1]['name']] = ''

        return form
    
    def get_table_page(self, form: dict) -> str:
        """
            Get page of packages
        """
        response = self.session.post(
            self.base_url+'/spedtabelas/AppConsulta/publico/aspx/ConsultaTabelasExternas.aspx?CodSistema=SpedFiscal', 
            data=form,
            headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        )
        
        assert response.ok, 'error to get table'
        
        return response.text
    
    def download_file_table(self, package_id: int, table_id: int) -> str:
        """
            Get file content
        """
        response = self.session.get(self.base_url+f'/spedtabelas/appconsulta/obterTabelaExterna.aspx?idPacote={package_id}&idTabela={table_id}')
        
        assert response.ok, 'error to download table'
        
        return response.text
    
    def extract(self) -> tuple:
        """
            get all
        """
        teste = Path('.')
        print('caminho atual da docker = ', teste.absolute())
        lake = Path('/tmp/datalake')
        lake.mkdir(exist_ok=True)
        
        soup = BeautifulSoup(self.get_package_page(), 'html.parser')
        
        # the form will be set before loop cause only two fields are changed in process
        form = self.get_form(soup)

        select_package = soup.find('select')
        for package_option in select_package.find_all('option'):
            package_id = package_option['value']
            package_name = package_option.string
            
            if package_id != '':
                # alter form package id 
                form[select_package['name']] = package_id

                soup = BeautifulSoup(self.get_table_page(form), 'html.parser')
                select_table = soup.find_all('select')
                for table_option in select_table[1].find_all('option'):
                    table_id = table_option['value']
                    table_name = table_option.string.replace('/', '-')
                    
                    if table_id != '':
                        fp = lake.joinpath(f'{package_name.strip()}___{table_name.strip()}.txt')
                        
                        with fp.open('w') as file:
                            file.write(self.download_file_table(package_id, table_id))
