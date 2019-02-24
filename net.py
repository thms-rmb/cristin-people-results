import concurrent.futures
import requests
import json

def get_people():
    def getter(url):
        resp = requests.get(url, timeout=10)
        if resp.status_code == requests.codes.ok:
            for person in resp.json():
                yield person

        if 'next' in resp.links:
            next_link = resp.links['next']['url']
            yield from getter(next_link)

    yield from getter('https://api.cristin.no/v2/persons?institution=dmmh')

def get_person_results(person, since = 2018, before = 2018):
    def getter(url):
        resp = requests.get(url, timeout=5)
        if resp.status_code == requests.codes.ok:
            for result in resp.json():
                yield result

        if 'next' in resp.links:
            next_link = resp.links['next']['url']
            yield from getter(next_link)

    person_id = person['cristin_person_id']
    yield from getter("{}?contributor={}&published_since={}&published_before={}".format(
        'https://api.cristin.no/v2/results', person_id, since, before
    ))

def filter_results(results):
    ids = set()
    for result in results:
        if result['cristin_result_id'] not in ids:
            ids.add(result['cristin_result_id'])
            yield result

def get_result_contributors(result):
    resp = requests.get(result['contributors']['url'], timeout=5)
    if resp.status_code == requests.codes.ok:
        for contributor in resp.json():
            yield contributor

def enrich_contributor(contributor):
    for affiliation in contributor['affiliations']:
        resp = requests.get(affiliation['institution']['url'], timeout=5)
        if resp.status_code == requests.codes.ok:
            institution = resp.json()
            affiliation['institution']['institution_name'] = institution['institution_name']
    return contributor

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        people = get_people()
        results = filter_results([result for results in pool.map(get_person_results, people) for result in results])
        contributors = map(get_result_contributors, results)
        
        for result, contributors in zip(results, contributors):
            for contributor in pool.map(enrich_contributor, contributors):
                for affiliation in contributor['affiliations']:
                    _, role = affiliation['role']['name'].popitem()
                    institution = affiliation['institution']['cristin_institution_id']
                    if 'institution_name' in affiliation['institution']:
                        _, institution = affiliation['institution']['institution_name'].popitem()

                    print("{},{},{},{}".format(result['cristin_result_id'], contributor['surname'], role, institution))
