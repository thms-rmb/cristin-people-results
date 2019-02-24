import sys, csv

import asyncio, aiohttp
from requests import codes
from requests.utils import parse_header_links

async def get_people(session):
    async def getter(url):
        async with session.get(url) as response:
            if response.status == codes.ok:
                for person in await response.json():
                    yield person

                links = parse_header_links(response.headers.get('link', ''))
                for link in links:
                    if link['rel'] == 'next':
                        async for person in getter(link['url']):
                            yield person
    async for person in getter('https://api.cristin.no/v2/persons?institution=dmmh'):
        yield person

async def get_person_results(session, person, since = 2018, before = 2018):
    async def getter(url):
        async with session.get(url) as response:
            if response.status == codes.ok:
                for result in await response.json():
                    yield result

                links = parse_header_links(response.headers.get('link', ''))
                for link in links:
                    if link['rel'] == 'next':
                        async for result in getter(link['url']):
                            yield result

    person_id = person['cristin_person_id']
    async for result in  getter("{}?contributor={}&published_since={}&published_before={}".format(
        'https://api.cristin.no/v2/results', person_id, since, before
    )):
        yield result

async def get_result_contributors(session, result):
    async with session.get(result['contributors']['url']) as response:
        if response.status == codes.ok:
            for contributor in await response.json():
                for affiliation in contributor['affiliations']:
                    await set_affiliation_institution(session, affiliation)
                    await set_affiliation_employment(session, contributor, affiliation)

                yield contributor

async def set_affiliation_institution(session, affiliation):
    async with session.get(affiliation['institution']['url']) as response:
        if response.status == codes.ok:
            institution = await response.json()
            affiliation['institution']['institution_name'] = institution['institution_name']

async def set_affiliation_employment(session, contributor, affiliation):
    async with session.get(contributor['url']) as response:
        if response.status == codes.ok:
            person = await response.json()
            if 'affiliations' in person:
                for employment in person['affiliations']:
                    if employment['institution']['cristin_institution_id'] == affiliation['institution']['cristin_institution_id']:
                        if employment['active']:
                            affiliation['institution']['employment'] = employment

async def get_result_affiliation_rows(session):
    results = set()
    async for person in get_people(session):
        async for result in get_person_results(session, person):
            if result['cristin_result_id'] in results:
                continue

            result_id = result['cristin_result_id']
            results.add(result_id)
            
            _, title = result['title'].popitem()
            _, category = result['category']['name'].popitem()
            async for contributor in get_result_contributors(session, result):
                surname, first_name = contributor['surname'], contributor['first_name']
                order = contributor['order']
                
                for affiliation in contributor['affiliations']:
                    _, role = affiliation['role']['name'].popitem()

                    institution = affiliation['institution']['cristin_institution_id']
                    if 'institution_name' in affiliation['institution']:
                        _, institution = affiliation['institution']['institution_name'].popitem()

                    employment = 'Unknown'
                    if 'employment' in affiliation['institution']:
                        if 'position' in affiliation['institution']['employment']:
                            _, employment = affiliation['institution']['employment']['position'].popitem()

                    yield result_id, title, category, surname, first_name, order, role, employment, institution
async def main():
    out = csv.writer(sys.stdout)
    out.writerow([
        'Id', 'Title', 'Category',
        'Surname', 'First Name',
        'Contribution Order Nr.', 'Role', 'Employment', 'Institution'
    ])
    
    async with aiohttp.ClientSession() as session:
        async for row in get_result_affiliation_rows(session):
            out.writerow(row)

if __name__ == '__main__':
    asyncio.run(main())
